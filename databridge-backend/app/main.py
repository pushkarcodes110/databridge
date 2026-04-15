import os
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.config import settings
from app.routers import upload, jobs, settings as settings_router, nocodb

app = FastAPI(title="DataBridge API", version="1.0")

app.include_router(upload.router)
app.include_router(jobs.router)
app.include_router(settings_router.router)
app.include_router(nocodb.router)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins.split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def read_root():
    return {"message": "DataBridge API is running"}


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/health/storage")
def storage_health():
    root = Path("/tmp/databridge")
    outputs = root / "outputs"
    uploads = root / "uploads"
    probe = root / ".backend-write-test"

    writable = False
    error = None
    try:
        root.mkdir(parents=True, exist_ok=True)
        probe.write_text("ok")
        probe.unlink(missing_ok=True)
        writable = True
    except Exception as exc:
        error = str(exc)

    return {
        "root": str(root),
        "root_exists": root.exists(),
        "root_realpath": os.path.realpath(root),
        "uploads_exists": uploads.exists(),
        "outputs_exists": outputs.exists(),
        "writable": writable,
        "error": error,
    }
