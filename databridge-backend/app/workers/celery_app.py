from celery import Celery
from app.config import settings

celery_app = Celery(
    "databridge_worker",
    broker=settings.redis_url,
    backend=settings.redis_url
)

celery_app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    worker_concurrency=settings.celery_concurrency
)

import app.workers.import_task  # ensure tasks are loaded
