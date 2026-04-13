import httpx
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.database import get_db
from pydantic import BaseModel
from typing import List, Optional
from app.models.settings import Settings as SettingsModel

router = APIRouter(prefix="/api/nocodb", tags=["NocoDB"])

async def get_nocodb_client(db: Session = Depends(get_db)):
    settings = db.query(SettingsModel).first()
    if not settings or not settings.nocodb_url or not settings.nocodb_api_token:
        raise HTTPException(status_code=400, detail="NocoDB credentials not configured in settings.")
    
    headers = {"xc-token": settings.nocodb_api_token}
    return settings.nocodb_url.rstrip("/"), headers

@router.get("/bases")
async def list_bases(client_info: tuple = Depends(get_nocodb_client)):
    url, headers = client_info
    async with httpx.AsyncClient() as client:
        # Try getting workspaces
        ws_endpoint = f"{url}/api/v1/workspaces"
        ws_res = await client.get(ws_endpoint, headers=headers)
        
        # If workspaces endpoint not found or forbidden, fallback to old API
        if ws_res.status_code in (404, 401, 403):
            endpoint = f"{url}/api/v1/db/meta/projects"
            res = await client.get(endpoint, headers=headers)
            res.raise_for_status()
            data = res.json()
            return data.get("list", data) if isinstance(data, dict) else data
            
        ws_res.raise_for_status()
        ws_data = ws_res.json()
        workspaces = ws_data.get("list", ws_data) if isinstance(ws_data, dict) else ws_data
        
        all_bases = []
        if isinstance(workspaces, list):
            for ws in workspaces:
                ws_id = ws.get("id")
                if ws_id:
                    base_end = f"{url}/api/v1/workspaces/{ws_id}/bases"
                    base_res = await client.get(base_end, headers=headers)
                    if base_res.status_code == 200:
                        b_data = base_res.json()
                        bases = b_data.get("list", b_data) if isinstance(b_data, dict) else b_data
                        if isinstance(bases, list):
                            all_bases.extend(bases)
        return all_bases

@router.get("/tables/{base_id}")
async def list_tables(base_id: str, client_info: tuple = Depends(get_nocodb_client)):
    url, headers = client_info
    endpoint = f"{url}/api/v1/db/meta/projects/{base_id}/tables"
    async with httpx.AsyncClient() as client:
        res = await client.get(endpoint, headers=headers)
        res.raise_for_status()
        data = res.json()
        return data.get("list", data) if isinstance(data, dict) else data

@router.get("/fields/{table_id}")
async def list_fields(table_id: str, base_id: Optional[str] = None, client_info: tuple = Depends(get_nocodb_client)):
    url, headers = client_info
    from app.services.nocodb import NocoDBClient
    
    # Use the service client which has all the fallbacks
    client = NocoDBClient(base_url=url, api_token=headers["xc-token"])
    try:
        fields = await client.get_table_fields(table_id, base_id=base_id)
        return fields
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        await client.close()

class CreateTableSchema(BaseModel):
    table_name: str
    columns: List[str]

@router.post("/tables/{base_id}")
async def create_table(base_id: str, data: CreateTableSchema, client_info: tuple = Depends(get_nocodb_client)):
    url, headers = client_info
    async with httpx.AsyncClient() as client:
        columns_payload = [
            {"column_name": "id", "title": "Id", "uidt": "ID", "pk": True, "ai": True}
        ]
        for col in data.columns:
            # Robust sanitization: only alphanumeric and underscores
            import re
            safe_col_name = re.sub(r'[^a-zA-Z0-9_]', '_', col.replace(" ", "_")).lower()
            if safe_col_name == "id" or not safe_col_name:
                safe_col_name = f"csv_{safe_col_name or 'col'}"
                
            columns_payload.append({
                "column_name": safe_col_name,
                "title": col,
                "uidt": "SingleLineText"
            })
            
        payload = {
            "title": data.table_name,
            "table_name": data.table_name.lower().replace(" ", "_"),
            "columns": columns_payload
        }
        
        endpoint = f"{url}/api/v2/meta/bases/{base_id}/tables"
        res = await client.post(endpoint, headers=headers, json=payload)
        
        if res.status_code == 404:
            endpoint = f"{url}/api/v1/db/meta/projects/{base_id}/tables"
            res = await client.post(endpoint, headers=headers, json=payload)
            
        res.raise_for_status()
        return res.json()
