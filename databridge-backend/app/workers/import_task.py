import os
import json
import asyncio
import pandas as pd
from datetime import datetime
from sqlalchemy.orm import Session
from app.workers.celery_app import celery_app
from app.database import SessionLocal
from app.models.job import ImportJob, ImportError
from app.models.settings import Settings
from app.services.nocodb import NocoDBClient
import redis

# Use direct import for settings if needed, but we init Redis here
from app.config import settings
redis_client = redis.Redis.from_url(settings.redis_url, decode_responses=True)

async def _process_import(job_id: str):
    db: Session = SessionLocal()
    job: ImportJob = db.query(ImportJob).filter(ImportJob.id == job_id).first()
    if not job:
        db.close()
        return

    job.status = "running"
    job.started_at = datetime.utcnow()
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
        if not await client.check_table_exists(job.nocodb_base_id, job.nocodb_table_id):
            raise Exception("Target NocoDB table does not exist or is inaccessible.")

        # Check fields and create missing ones
        target_fields = await client.get_table_fields(job.nocodb_table_id)
        existing_field_names = [f["title"] for f in target_fields]
        
        # We need to map columns. For MVP we assume mapping handles existing vs non-existing
        # Or at least check the first chunk headers.
        
        # Read file chunks
        file_path = job.file_path
        ext = os.path.splitext(file_path)[1].lower()
        
        chunksize = 1000
        inserted_rows = 0
        failed_rows = 0
        total_rows = job.total_rows or 0 # should be estimated earlier optimally
        
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
                        new_row[noco_field] = val
                processed_records.append(new_row)

            # Auto-create missing columns in NocoDB before insert
            if chunk_idx == 0 and processed_records:
                for noco_field in processed_records[0].keys():
                    if noco_field not in existing_field_names:
                        await client.create_field(job.nocodb_table_id, {"title": noco_field, "uidt": "SingleLineText"})
                        existing_field_names.append(noco_field)
            
            # Dispatch parallel batches of 100 max
            batches = [processed_records[i:i+100] for i in range(0, len(processed_records), 100)]
            tasks = []
            for batch in batches:
                tasks.append(client.bulk_insert(job.nocodb_base_id, job.nocodb_table_id, batch))
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Handle results
            for batch_records, result in zip(batches, results):
                if isinstance(result, Exception):
                    failed_rows += len(batch_records)
                    # Log errors
                    # In a real app we'd map original row numbers, simplified here
                    db.add(ImportError(job_id=job.id, row_number=0, row_data=batch_records[0], error_message=str(result)))
                else:
                    inserted_rows += len(batch_records)

            # Update progress
            job.inserted_rows = inserted_rows
            job.failed_rows = failed_rows
            db.commit()
            
            # Update Redis cache for fast polling
            progress_data = {"inserted": inserted_rows, "failed": failed_rows, "total": total_rows}
            redis_client.set(f"job:{job.id}:progress", json.dumps(progress_data), ex=3600)

        job.status = "complete" if failed_rows == 0 else "failed"
        
    except Exception as e:
        job.status = "failed"
        job.error_summary = str(e)
    finally:
        job.completed_at = datetime.utcnow()
        db.commit()
        db.close()
        await client.close()


@celery_app.task(name="import_task")
def run_import_job(job_id: str):
    """
    Synchronous wrapper for Celery to call the async import process.
    """
    asyncio.run(_process_import(job_id))
