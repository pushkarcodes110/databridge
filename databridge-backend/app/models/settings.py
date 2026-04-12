import uuid
from sqlalchemy import Column, String, Integer, JSON
from app.database import Base

class Settings(Base):
    __tablename__ = "settings"
    
    id = Column(String, primary_key=True, default="default")
    nocodb_url = Column(String, nullable=True)
    nocodb_api_token = Column(String, nullable=True) # Will be encrypted
    default_concurrency = Column(Integer, default=5)
    table_presets = Column(JSON, default=[]) # array of {name, base_id, table_id}
