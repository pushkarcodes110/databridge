from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    database_url: str = "postgresql://user:password@localhost:5432/databridge"
    redis_url: str = "redis://localhost:6379/0"
    environment: str = "development"
    upload_dir: str = "uploads"
    encryption_key: str = "supersecretkey-change-me-in-production="
    celery_concurrency: int = 5
    cors_origins: str = "http://localhost:3000"
    max_upload_size_mb: int = 500

    class Config:
        env_file = ".env"

settings = Settings()
