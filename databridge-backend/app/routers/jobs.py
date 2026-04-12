import json
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.database import get_db
from app.models.job import ImportJob
from app.workers.import_task import run_import_job
from pydantic import BaseModel
from typing import Dict, Any, Optional

import redis
from app.config import settings

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

@router.post("/")
def create_job(request: JobCreateRequest, db: Session = Depends(get_db)):
    job = ImportJob(
        filename=request.filename,
        file_path=request.file_path,
        file_size=request.file_size,
        file_format=request.file_format,
        nocodb_base_id=request.nocodb_base_id,
        nocodb_table_id=request.nocodb_table_id,
        nocodb_url=request.nocodb_url,
        column_mapping=request.column_mapping,
        options=request.options,
        status="pending"
    )
    db.add(job)
    db.commit()
    db.refresh(job)

    # push to Celery
    run_import_job.delay(job.id)
    
    return {"id": job.id, "status": "pending"}

@router.get("/{job_id}/progress")
def get_job_progress(job_id: str, db: Session = Depends(get_db)):
    job = db.query(ImportJob).filter(ImportJob.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
        
    # Check redis first for live progress
    progress_str = redis_client.get(f"job:{job_id}:progress")
    if progress_str:
        progress_data = json.loads(progress_str)
        return {
            "status": job.status,
            "inserted": progress_data.get("inserted", 0),
            "failed": progress_data.get("failed", 0),
            "total": progress_data.get("total", job.total_rows)
        }
    
    # Fallback to DB
    return {
        "status": job.status,
        "inserted": job.inserted_rows,
        "failed": job.failed_rows,
        "total": job.total_rows
    }
