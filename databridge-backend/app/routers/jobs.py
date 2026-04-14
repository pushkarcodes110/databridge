import json
import os
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.database import get_db
from app.models.job import ImportJob, ImportError
from app.workers.import_task import run_import_job
from pydantic import BaseModel
from typing import Dict, Any, Optional, List

import redis
from app.config import settings
from app.workers.celery_app import celery_app

redis_client = redis.Redis.from_url(settings.redis_url, decode_responses=True)

router = APIRouter(prefix="/api/jobs", tags=["Jobs"])

class JobCreateRequest(BaseModel):
    filename: str
    file_path: str
    file_size: int
    file_format: str
    nocodb_base_id: str
    nocodb_table_id: str
    nocodb_url: Optional[str] = None
    column_mapping: Dict[str, str] = {}
    options: Dict[str, Any] = {}
    total_rows: Optional[int] = 0


def _get_live_progress(job: ImportJob) -> Dict[str, Any]:
    progress_str = redis_client.get(f"job:{job.id}:progress")
    progress_data = json.loads(progress_str) if progress_str else {}
    total_hint = job.total_rows or job.file_size or 0

    inserted = progress_data.get("inserted", job.inserted_rows or 0)
    failed = progress_data.get("failed", job.failed_rows or 0)
    total = progress_data.get("total") or total_hint
    processed = inserted + failed
    progress_percent = round((processed / total) * 100, 2) if total else 0

    return {
        "inserted": inserted,
        "failed": failed,
        "total": total,
        "progress_percent": progress_percent,
    }


def _serialize_job(job: ImportJob, db: Session, include_errors: bool = False) -> Dict[str, Any]:
    progress = _get_live_progress(job)
    total = progress["total"]
    processed = progress["inserted"] + progress["failed"]
    is_partial_complete = job.status == "complete" and total and processed < total
    status = "failed" if is_partial_complete else job.status
    error_summary = job.error_summary
    if is_partial_complete and not error_summary:
        error_summary = (
            f"Import stopped before all rows were processed: {processed} of {total} rows handled. "
            "Use Resume to continue from the last saved progress."
        )

    queue_position = None
    if status == "pending":
        queue_position = (
            db.query(ImportJob)
            .filter(ImportJob.status == "pending", ImportJob.created_at < job.created_at)
            .count()
            + 1
        )

    payload = {
        "id": job.id,
        "status": status,
        "filename": job.filename,
        "file_format": job.file_format,
        "file_size": job.file_size,
        "nocodb_base_id": job.nocodb_base_id,
        "nocodb_table_id": job.nocodb_table_id,
        "inserted": progress["inserted"],
        "failed": progress["failed"],
        "total": progress["total"],
        "progress_percent": progress["progress_percent"],
        "error_summary": error_summary,
        "started_at": job.started_at.isoformat() if job.started_at else None,
        "completed_at": job.completed_at.isoformat() if job.completed_at else None,
        "created_at": job.created_at.isoformat() if job.created_at else None,
        "queue_position": queue_position,
        "can_cancel": status in {"pending", "running"},
        "can_resume": status in {"failed", "cancelled", "complete"} and (
            (job.inserted_rows or 0) + (job.failed_rows or 0)
        ) < (job.total_rows or job.file_size or 0),
    }

    if include_errors:
        errors: List[ImportError] = (
            db.query(ImportError)
            .filter(ImportError.job_id == job.id)
            .order_by(ImportError.created_at.desc())
            .limit(25)
            .all()
        )
        payload["errors"] = [
            {
                "id": error.id,
                "row_number": error.row_number,
                "row_data": error.row_data,
                "error_message": error.error_message,
                "created_at": error.created_at.isoformat() if error.created_at else None,
            }
            for error in errors
        ]
        payload["error_count"] = (
            db.query(ImportError).filter(ImportError.job_id == job.id).count()
        )

    return payload

@router.post("/")
def create_job(request: JobCreateRequest, db: Session = Depends(get_db)):
    stored_file_path = request.file_path
    if stored_file_path and not os.path.isabs(stored_file_path):
        stored_file_path = os.path.join(settings.upload_dir, stored_file_path)

    job = ImportJob(
        filename=request.filename,
        file_path=stored_file_path,
        file_size=request.file_size,
        file_format=request.file_format,
        nocodb_base_id=request.nocodb_base_id,
        nocodb_table_id=request.nocodb_table_id,
        nocodb_url=request.nocodb_url,
        column_mapping=request.column_mapping,
        options=dict(request.options or {}),
        total_rows=request.total_rows or request.file_size or 0,
        status="pending"
    )
    db.add(job)
    db.commit()
    db.refresh(job)

    # push to Celery
    task = run_import_job.delay(job.id)
    job.options = {**(job.options or {}), "celery_task_id": task.id}
    db.commit()

    redis_client.set(
        f"job:{job.id}:progress",
        json.dumps({"inserted": 0, "failed": 0, "total": job.total_rows or 0}),
        ex=3600,
    )
    
    return {"id": job.id, "status": "pending"}


@router.get("/")
def list_jobs(limit: int = 50, db: Session = Depends(get_db)):
    jobs = (
        db.query(ImportJob)
        .order_by(ImportJob.created_at.desc())
        .limit(min(max(limit, 1), 200))
        .all()
    )
    return [_serialize_job(job, db, include_errors=False) for job in jobs]


@router.get("/{job_id}")
def get_job(job_id: str, db: Session = Depends(get_db)):
    job = db.query(ImportJob).filter(ImportJob.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return _serialize_job(job, db, include_errors=True)

@router.get("/{job_id}/progress")
def get_job_progress(job_id: str, db: Session = Depends(get_db)):
    job = db.query(ImportJob).filter(ImportJob.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return _serialize_job(job, db, include_errors=False)


@router.post("/{job_id}/cancel")
def cancel_job(job_id: str, db: Session = Depends(get_db)):
    job = db.query(ImportJob).filter(ImportJob.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.status in {"complete", "failed", "cancelled"}:
        return _serialize_job(job, db, include_errors=False)

    job.status = "cancelled"
    task_id = (job.options or {}).get("celery_task_id")
    if task_id:
        celery_app.control.revoke(task_id, terminate=False)

    redis_client.set(
        f"job:{job.id}:progress",
        json.dumps(
            {
                "inserted": job.inserted_rows or 0,
                "failed": job.failed_rows or 0,
                "total": job.total_rows or job.file_size or 0,
            }
        ),
        ex=3600,
    )

    db.commit()
    db.refresh(job)
    return _serialize_job(job, db, include_errors=False)


@router.post("/{job_id}/resume")
def resume_job(job_id: str, db: Session = Depends(get_db)):
    job = db.query(ImportJob).filter(ImportJob.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.status in {"pending", "running"}:
        return _serialize_job(job, db, include_errors=False)

    processed_rows = (job.inserted_rows or 0) + (job.failed_rows or 0)
    total_rows = job.total_rows or job.file_size or 0
    if total_rows and processed_rows >= total_rows:
        return _serialize_job(job, db, include_errors=False)

    job.status = "pending"
    job.completed_at = None
    job.error_summary = None
    job.options = {
        **(job.options or {}),
        "resume": True,
        "resume_from_row": processed_rows,
    }
    db.commit()
    db.refresh(job)

    task = run_import_job.delay(job.id)
    job.options = {**(job.options or {}), "celery_task_id": task.id}
    db.commit()

    redis_client.set(
        f"job:{job.id}:progress",
        json.dumps(
            {
                "inserted": job.inserted_rows or 0,
                "failed": job.failed_rows or 0,
                "total": total_rows,
            }
        ),
        ex=3600,
    )

    db.refresh(job)
    return _serialize_job(job, db, include_errors=False)
