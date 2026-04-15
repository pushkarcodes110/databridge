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
