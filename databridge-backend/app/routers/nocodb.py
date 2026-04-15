import httpx
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.database import get_db
from pydantic import BaseModel
from typing import List, Optional
from app.models.settings import Settings as SettingsModel
import re

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

def sanitize_table_name(value: str) -> str:
    table_name = re.sub(r"[^a-zA-Z0-9_]", "_", value.strip().replace(" ", "_")).lower()
    table_name = re.sub(r"_+", "_", table_name).strip("_")
    if not table_name:
        table_name = "databridge_import"
    if table_name[0].isdigit():
        table_name = f"t_{table_name}"
    return table_name[:60]

def unique_column_title(value: str, seen_titles: set[str], index: int) -> str:
    title = (value or "").strip() or f"Column {index + 1}"
    title = title[:250]
    base = title
    suffix = 2
    while title.lower() in seen_titles:
        title = f"{base[:240]} {suffix}"
        suffix += 1
    seen_titles.add(title.lower())
    return title

def sanitize_column_name(value: str, seen_names: set[str], index: int) -> str:
    column_name = re.sub(r"[^a-zA-Z0-9_]", "_", value.strip().replace(" ", "_")).lower()
    column_name = re.sub(r"_+", "_", column_name).strip("_")
    if not column_name or column_name == "id":
        column_name = f"csv_col_{index + 1}"
    if column_name[0].isdigit():
        column_name = f"c_{column_name}"
    column_name = column_name[:60]

    base = column_name
    suffix = 2
    while column_name in seen_names:
        trimmed = base[: max(1, 60 - len(str(suffix)) - 1)]
        column_name = f"{trimmed}_{suffix}"
        suffix += 1
    seen_names.add(column_name)
    return column_name

def noco_error_detail(response: httpx.Response) -> str:
    try:
        payload = response.json()
    except ValueError:
        payload = response.text
    return f"NocoDB table creation failed ({response.status_code}): {payload}"

@router.post("/tables/{base_id}")
async def create_table(base_id: str, data: CreateTableSchema, client_info: tuple = Depends(get_nocodb_client)):
    url, headers = client_info
    async with httpx.AsyncClient() as client:
        seen_titles: set[str] = set()
        seen_names: set[str] = set()
        columns_payload = []
        for index, col in enumerate(data.columns):
            title = unique_column_title(col, seen_titles, index)
            column_name = sanitize_column_name(title, seen_names, index)
            columns_payload.append({
                "column_name": column_name,
                "title": title,
                "uidt": "SingleLineText"
            })

        table_name = sanitize_table_name(data.table_name)
        payload = {
            "title": data.table_name.strip() or table_name,
            "table_name": table_name,
            "columns": columns_payload
        }
        
        endpoint = f"{url}/api/v2/meta/bases/{base_id}/tables"
        res = await client.post(endpoint, headers=headers, json=payload)

        # Some NocoDB versions reject explicit database column names during v2
        # table creation. Retry with title-only columns before falling back.
        if res.status_code in (400, 422):
            title_only_payload = {
                **payload,
                "columns": [
                    {"title": column["title"], "uidt": column["uidt"]}
                    for column in columns_payload
                ],
            }
            retry_res = await client.post(endpoint, headers=headers, json=title_only_payload)
            if retry_res.status_code < 400:
                return retry_res.json()
            res = retry_res
        
        if res.status_code == 404:
            endpoint = f"{url}/api/v1/db/meta/projects/{base_id}/tables"
            res = await client.post(endpoint, headers=headers, json=payload)

        if res.status_code >= 400:
            raise HTTPException(status_code=res.status_code, detail=noco_error_detail(res))

        return res.json()
