import uuid
from sqlalchemy import Column, String, Integer, DateTime, JSON, ForeignKey, Text
from sqlalchemy.orm import relationship
from datetime import datetime
from app.database import Base

class ImportJob(Base):
    __tablename__ = "import_jobs"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    status = Column(String, default="pending") # pending, running, complete, failed, cancelled
    filename = Column(String, nullable=False)
    file_path = Column(String, nullable=False)
    file_size = Column(Integer, nullable=False)
    file_format = Column(String, nullable=False)
    
    nocodb_base_id = Column(String, nullable=True)
    nocodb_table_id = Column(String, nullable=True)
    nocodb_url = Column(String, nullable=True)
    
    column_mapping = Column(JSON, default={})
    options = Column(JSON, default={})
    
    total_rows = Column(Integer, default=0)
    inserted_rows = Column(Integer, default=0)
    failed_rows = Column(Integer, default=0)
    
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    error_summary = Column(Text, nullable=True)
    
    errors = relationship("ImportError", back_populates="job", cascade="all, delete-orphan")

class ImportError(Base):
    __tablename__ = "import_errors"
    
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    job_id = Column(String, ForeignKey("import_jobs.id"))
    row_number = Column(Integer, nullable=False)
    row_data = Column(JSON, nullable=False)
    error_message = Column(Text, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    job = relationship("ImportJob", back_populates="errors")
