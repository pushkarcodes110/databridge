import os
import json
import asyncio
import pandas as pd
import math
from datetime import datetime
from sqlalchemy.orm import Session
from app.workers.celery_app import celery_app
from app.database import SessionLocal
from app.models.job import ImportJob, ImportError
from app.models.settings import Settings
from app.services.nocodb import NocoDBClient
import redis
import importlib
import app.services.nocodb as nocodb_service

# Use direct import for settings if needed, but we init Redis here
from app.config import settings
redis_client = redis.Redis.from_url(settings.redis_url, decode_responses=True)


def _resolve_upload_path(file_path: str) -> str:
    if os.path.isabs(file_path):
        return file_path
    normalized = os.path.normpath(file_path)
    upload_root = os.path.normpath(settings.upload_dir)
    if normalized == upload_root or normalized.startswith(f"{upload_root}{os.sep}"):
        return normalized
    return os.path.join(settings.upload_dir, file_path)


def _json_safe_value(value):
    if isinstance(value, pd.Timestamp):
        return value.isoformat()

    if value is None:
        return None

    if isinstance(value, float) and math.isnan(value):
        return None

    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    if hasattr(value, "item") and callable(value.item):
        try:
            return _json_safe_value(value.item())
        except Exception:
            pass

    if isinstance(value, dict):
        return {str(k): _json_safe_value(v) for k, v in value.items()}

    if isinstance(value, (list, tuple, set)):
        return [_json_safe_value(v) for v in value]

    if isinstance(value, (str, int, bool, float)):
        return value

    if isinstance(value, datetime):
        return value.isoformat()

    return str(value)


def _set_progress(job_id: str, inserted: int, failed: int, total: int) -> None:
    redis_client.set(
        f"job:{job_id}:progress",
        json.dumps({"inserted": inserted, "failed": failed, "total": total}),
        ex=3600,
    )


def _refresh_job(db: Session, job: ImportJob) -> ImportJob:
    db.refresh(job)
    return job


def _get_resume_offset(job: ImportJob) -> int:
    options = job.options or {}
    if not options.get("resume"):
        return 0
    if options.get("resume_from_row") is not None:
        return max(int(options.get("resume_from_row") or 0), 0)
    return max((job.inserted_rows or 0) + (job.failed_rows or 0), 0)

async def _process_import(job_id: str):
    # Hot-reload the service to pick up 404 fallback fixes without worker restart
    importlib.reload(nocodb_service)
    from app.services.nocodb import NocoDBClient
    
    db: Session = SessionLocal()
    job: ImportJob = db.query(ImportJob).filter(ImportJob.id == job_id).first()
    if not job:
        db.close()
        return

    resume_offset = _get_resume_offset(job)
    job.status = "running"
    job.started_at = job.started_at or datetime.utcnow()
    job.completed_at = None
    job.error_summary = None
    if resume_offset == 0:
        job.inserted_rows = 0
        job.failed_rows = 0
    db.commit()

    # Get settings for credentials
    app_settings = db.query(Settings).first()
    if not app_settings or not app_settings.nocodb_api_token:
        job.status = "failed"
        job.error_summary = "NocoDB API Token is not configured in settings."
        db.commit()
        db.close()
        return

    client = NocoDBClient(
        base_url=job.nocodb_url or app_settings.nocodb_url, 
        api_token=app_settings.nocodb_api_token,
        max_concurrent=app_settings.default_concurrency
    )

    try:
        _set_progress(job.id, job.inserted_rows or 0, job.failed_rows or 0, job.total_rows or job.file_size or 0)

        if not await client.check_table_exists(job.nocodb_base_id, job.nocodb_table_id):
            raise Exception("Target NocoDB table does not exist or is inaccessible.")

        # New tables on this NocoDB instance return 404 for all table-field metadata
        # endpoints we probed. Import should continue even when schema introspection
        # is unavailable, as long as the mapped keys already match the target titles.
        target_fields = await client.get_table_fields(job.nocodb_table_id, base_id=job.nocodb_base_id)
        existing_field_names = [f["title"] for f in target_fields if f.get("title")]
        schema_introspection_available = len(existing_field_names) > 0
        
        # We need to map columns. For MVP we assume mapping handles existing vs non-existing
        # Or at least check the first chunk headers.
        
        # Read file chunks
        file_path = _resolve_upload_path(job.file_path)
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Uploaded file not found at {file_path}")
        ext = os.path.splitext(file_path)[1].lower()
        
        chunksize = 1000
        inserted_rows = job.inserted_rows or 0
        failed_rows = job.failed_rows or 0
        total_rows = job.total_rows or job.file_size or 0
        actual_rows_seen = 0
        
        if ext == ".csv":
            reader = pd.read_csv(file_path, chunksize=chunksize)
        elif ext in [".xlsx", ".xls"]:
            # openpyxl doesn't support true chunking natively via pandas easily without reading all,
            # but we assume size is reasonable or handle by skipping rows
            df = pd.read_excel(file_path)
            reader = [df[i:i+chunksize] for i in range(0, df.shape[0], chunksize)]
        elif ext == ".json":
            df = pd.read_json(file_path)
            reader = [df[i:i+chunksize] for i in range(0, df.shape[0], chunksize)]
        else:
            raise ValueError("Unsupported format")

        for chunk_idx, df_chunk in enumerate(reader):
            job = _refresh_job(db, job)
            if job.status == "cancelled":
                break

            actual_rows_seen += len(df_chunk)
            chunk_start = chunk_idx * chunksize
            chunk_end = chunk_start + len(df_chunk)
            if resume_offset >= chunk_end:
                continue
            if resume_offset > chunk_start:
                df_chunk = df_chunk.iloc[resume_offset - chunk_start:]

            df_chunk = df_chunk.fillna("")
            records = df_chunk.to_dict(orient="records")
            
            # Map columns and create missing fields dynamically based on mapping dict
            mapping = job.column_mapping or {}
            processed_records = []
            
            for row in records:
                new_row = {}
                for csv_col, val in row.items():
                    noco_field = mapping.get(csv_col, csv_col)
                    if noco_field: # If not skipped
                        new_row[noco_field] = _json_safe_value(val)
                processed_records.append(new_row)

            # Only mutate schema if we could actually inspect the table schema.
            if chunk_idx == 0 and processed_records and schema_introspection_available:
                for noco_field in processed_records[0].keys():
                    if noco_field not in existing_field_names:
                        await client.create_field(job.nocodb_table_id, {"title": noco_field, "uidt": "SingleLineText"})
                        existing_field_names.append(noco_field)
            
            # Dispatch parallel batches of 100 max
            batches = [processed_records[i:i+100] for i in range(0, len(processed_records), 100)]
            
            async def run_batch(batch_records):
                try:
                    result = await client.bulk_insert(job.nocodb_base_id, job.nocodb_table_id, batch_records)
                    return batch_records, result, None
                except Exception as exc:
                    return batch_records, None, exc

            tasks = [asyncio.create_task(run_batch(batch)) for batch in batches]

            for task in asyncio.as_completed(tasks):
                batch_records, result, error = await task
                try:
                    if error is not None:
                        raise error
                except Exception as result:
                    failed_rows += len(batch_records)
                    db.rollback()
                    # Log errors
                    # In a real app we'd map original row numbers, simplified here
                    db.add(
                        ImportError(
                            job_id=job.id,
                            row_number=0,
                            row_data=_json_safe_value(batch_records[0]),
                            error_message=str(result),
                        )
                    )
                else:
                    inserted_rows += len(batch_records)

                job.inserted_rows = inserted_rows
                job.failed_rows = failed_rows
                db.commit()
                _set_progress(job.id, inserted_rows, failed_rows, total_rows)

            job = _refresh_job(db, job)
            if job.status == "cancelled":
                break

        if job.status != "cancelled" and actual_rows_seen and total_rows != actual_rows_seen:
            total_rows = actual_rows_seen
            job.total_rows = actual_rows_seen
            db.commit()
            _set_progress(job.id, inserted_rows, failed_rows, total_rows)

        if job.status == "cancelled":
            job.error_summary = "Import cancelled by user."
        elif total_rows and inserted_rows + failed_rows < total_rows:
            job.status = "failed"
            job.error_summary = (
                f"Import stopped before all rows were processed: "
                f"{inserted_rows + failed_rows} of {total_rows} rows handled. "
                "Use Resume to continue from the last saved progress."
            )
        else:
            job.status = "complete" if failed_rows == 0 else "failed"
        
    except Exception as e:
        job.status = "failed"
        job.error_summary = str(e)
    finally:
        job.completed_at = datetime.utcnow()
        db.commit()
        _set_progress(job.id, job.inserted_rows or 0, job.failed_rows or 0, job.total_rows or job.file_size or 0)
        db.close()
        await client.close()


@celery_app.task(name="import_task")
def run_import_job(job_id: str):
    """
    Synchronous wrapper for Celery to call the async import process.
    """
    asyncio.run(_process_import(job_id))
