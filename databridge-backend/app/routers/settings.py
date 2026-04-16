import httpx
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from pydantic import BaseModel
from typing import List, Dict, Any, Optional

from app.database import get_db
from app.models.settings import Settings as SettingsModel

router = APIRouter(prefix="/api/settings", tags=["Settings"])

class TablePreset(BaseModel):
    name: str
    base_id: str
    table_id: str

class SettingsSchema(BaseModel):
    nocodb_url: Optional[str] = None
    nocodb_api_token: Optional[str] = None
    base_id: Optional[str] = None
    webhook_enabled: bool = False
    webhook_url: Optional[str] = None
    webhook_batch_size: int = 500
    default_concurrency: int = 5
    table_presets: List[TablePreset] = []

@router.get("/", response_model=SettingsSchema)
def get_settings(db: Session = Depends(get_db)):
    settings_obj = db.query(SettingsModel).first()
    if not settings_obj:
        settings_obj = SettingsModel()
        db.add(settings_obj)
        db.commit()
        db.refresh(settings_obj)
        
    return {
        "nocodb_url": settings_obj.nocodb_url,
        "nocodb_api_token": "********" if settings_obj.nocodb_api_token else None,
        "base_id": settings_obj.base_id,
        "webhook_enabled": bool(settings_obj.webhook_enabled),
        "webhook_url": settings_obj.webhook_url,
        "webhook_batch_size": settings_obj.webhook_batch_size or 500,
        "default_concurrency": settings_obj.default_concurrency,
        "table_presets": settings_obj.table_presets
    }

@router.put("/", response_model=SettingsSchema)
def update_settings(settings_in: SettingsSchema, db: Session = Depends(get_db)):
    settings_obj = db.query(SettingsModel).first()
    if not settings_obj:
        settings_obj = SettingsModel()
        db.add(settings_obj)
    
    settings_obj.nocodb_url = settings_in.nocodb_url
    if settings_in.nocodb_api_token and settings_in.nocodb_api_token != "********":
        settings_obj.nocodb_api_token = settings_in.nocodb_api_token

    settings_obj.base_id = settings_in.base_id
    settings_obj.webhook_enabled = 1 if settings_in.webhook_enabled else 0
    settings_obj.webhook_url = settings_in.webhook_url.strip() if settings_in.webhook_url else None
    settings_obj.webhook_batch_size = max(1, min(settings_in.webhook_batch_size or 500, 2000))
    settings_obj.default_concurrency = settings_in.default_concurrency
    settings_obj.table_presets = [p.dict() for p in settings_in.table_presets]
    
    db.commit()
    db.refresh(settings_obj)
    
    return {
        "nocodb_url": settings_obj.nocodb_url,
        "nocodb_api_token": "********" if settings_obj.nocodb_api_token else None,
        "base_id": settings_obj.base_id,
        "webhook_enabled": bool(settings_obj.webhook_enabled),
        "webhook_url": settings_obj.webhook_url,
        "webhook_batch_size": settings_obj.webhook_batch_size or 500,
        "default_concurrency": settings_obj.default_concurrency,
        "table_presets": settings_obj.table_presets
    }

@router.post("/test")
async def test_connection(settings_in: SettingsSchema, db: Session = Depends(get_db)):
    if not settings_in.nocodb_url or not settings_in.nocodb_api_token:
        raise HTTPException(status_code=400, detail="URL and API Token are required to test connection.")
        
    actual_token = settings_in.nocodb_api_token
    if actual_token == "********":
        settings_obj = db.query(SettingsModel).first()
        if settings_obj and settings_obj.nocodb_api_token:
            actual_token = settings_obj.nocodb_api_token
        else:
            return {"status": "error", "message": "No valid API token found in settings."}

    try:
        url = f"{settings_in.nocodb_url.rstrip('/')}/api/v1/workspaces"
        headers = {"xc-token": actual_token}
        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=headers, timeout=10.0)
            
        if response.status_code == 200:
            return {"status": "success", "message": "Connection successful"}
        else:
            return {"status": "error", "message": f"Connection failed. Status: {response.status_code}"}
    except Exception as e:
         return {"status": "error", "message": str(e)}
