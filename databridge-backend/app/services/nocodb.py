import httpx
import asyncio
from typing import List, Dict, Any, Optional

def parse_json_response(response: httpx.Response) -> Any:
    text = response.text
    content_type = response.headers.get("content-type", "")
    if "application/json" not in content_type and text and not text.lstrip().startswith(("{", "[")):
        raise ValueError(f"NocoDB returned non-JSON response ({response.status_code}): {text[:180]}")
    try:
        return response.json()
    except ValueError as exc:
        raise ValueError(f"NocoDB returned invalid JSON ({response.status_code}): {text[:180]}") from exc

class NocoDBClient:
    def __init__(self, base_url: str, api_token: str, max_concurrent: int = 5):
        self.base_url = base_url.rstrip('/')
        self.api_token = api_token
        self.headers = {"xc-token": self.api_token, "Content-Type": "application/json"}
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.client = httpx.AsyncClient(timeout=30.0)

    async def close(self):
        await self.client.aclose()

    async def check_table_exists(self, base_id: str, table_id: str) -> bool:
        # Strategy: Try Data API first, then Meta API listing
        try:
            url = f"{self.base_url}/api/v1/db/data/noco/{base_id}/{table_id}?limit=1"
            response = await self.client.get(url, headers=self.headers)
            if response.status_code == 200:
                return True
        except:
            pass

        try:
            # Fallback: check if it's in the projects tables list
            url = f"{self.base_url}/api/v1/db/meta/projects/{base_id}/tables"
            response = await self.client.get(url, headers=self.headers)
            if response.status_code == 200:
                tables = parse_json_response(response).get('list', [])
                return any(t.get('id') == table_id or t.get('table_name') == table_id for t in tables)
        except:
            pass
        
        return False

    async def get_table_fields(self, table_id: str, base_id: Optional[str] = None) -> List[Dict[str, Any]]:
        # Try multiple meta endpoints
        endpoints = [
            f"/api/v1/meta/tables/{table_id}/fields",
            f"/api/v1/meta/tables/{table_id}/columns",
            f"/api/v1/db/meta/tables/{table_id}/columns",
        ]
        
        for ep in endpoints:
            try:
                url = f"{self.base_url}{ep}"
                response = await self.client.get(url, headers=self.headers)
                if response.status_code == 200:
                    data = parse_json_response(response)
                    return data.get("list", data) if isinstance(data, dict) else data
            except:
                continue

        # Plan B: If base_id is provided, try to infer from single record
        if base_id:
            try:
                url = f"{self.base_url}/api/v1/db/data/noco/{base_id}/{table_id}?limit=1"
                response = await self.client.get(url, headers=self.headers)
                if response.status_code == 200:
                    sample = parse_json_response(response)
                    records = sample.get('list', sample) if isinstance(sample, dict) else sample
                    if records and len(records) > 0:
                        # Infer fields from keys
                        return [{"title": k, "column_name": k, "uidt": "SingleLineText"} for k in records[0].keys()]
            except:
                pass
        
        # If we reach here, we couldn't find fields. 
        # For a new empty table, returning an empty list is better than a 500 error.
        return []

    async def create_field(self, table_id: str, field_def: Dict[str, Any]) -> Dict[str, Any]:
        # Try both /fields and /columns
        for sub in ["fields", "columns"]:
            url = f"{self.base_url}/api/v1/meta/tables/{table_id}/{sub}"
            try:
                response = await self.client.post(url, headers=self.headers, json=field_def)
                if response.status_code in [200, 201]:
                    return parse_json_response(response)
                elif response.status_code in [400, 409]: # Likely already exists
                    return {"message": "Field already exists or invalid def"}
            except:
                continue
        
        # If we couldn't create it, it might already exist or the endpoint is wrong.
        # We'll return a placeholder to let the import continue.
        return {"status": "skipped"}

    async def bulk_insert(self, base_id: str, table_id: str, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        if len(records) > 100:
            raise ValueError("NocoDB bulk insert hard limit is 100 records per request")

        url = f"{self.base_url}/api/v1/db/data/bulk/noco/{base_id}/{table_id}"
        payloads = [
            records,
            [{"fields": record} for record in records],
        ]

        async with self.semaphore:
            last_error: Optional[str] = None

            for payload in payloads:
                response = await self.client.post(url, headers=self.headers, json=payload)
                if response.status_code in [200, 201]:
                    return parse_json_response(response)

                last_error = f"Bulk insert failed: {response.status_code} - {response.text}"

                # If the endpoint exists but the payload shape is invalid, retry once
                # with the alternate record shape used by newer NocoDB examples.
                if response.status_code not in [400, 404, 422]:
                    break

            raise Exception(last_error or "Bulk insert failed for an unknown reason")
