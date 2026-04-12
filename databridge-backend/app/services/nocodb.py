import httpx
import asyncio
from typing import List, Dict, Any, Optional

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
        url = f"{self.base_url}/api/v1/db/data/noco/{base_id}/{table_id}?limit=1"
        response = await self.client.get(url, headers=self.headers)
        return response.status_code == 200

    async def get_table_fields(self, table_id: str) -> List[Dict[str, Any]]:
        url = f"{self.base_url}/api/v1/meta/tables/{table_id}/columns"
        # NocoDB v1 may have different paths for meta fields, assuming /columns based on usual patterns
        # Wait, the PRD says: /api/v1/meta/tables/{tableId}/fields
        url = f"{self.base_url}/api/v1/meta/tables/{table_id}/fields"
        response = await self.client.get(url, headers=self.headers)
        response.raise_for_status()
        # the response might be wrapped, usually it's just a JSON array or { "list": [...] }
        data = response.json()
        return data.get("list", data) if isinstance(data, dict) else data

    async def create_field(self, table_id: str, field_def: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.base_url}/api/v1/meta/tables/{table_id}/fields"
        response = await self.client.post(url, headers=self.headers, json=field_def)
        if response.status_code not in [200, 201]:
            raise Exception(f"Failed to create field: {response.text}")
        return response.json()

    async def bulk_insert(self, base_id: str, table_id: str, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        if len(records) > 100:
            raise ValueError("NocoDB bulk insert hard limit is 100 records per request")
            
        url = f"{self.base_url}/api/v1/db/data/bulk/noco/{base_id}/{table_id}"
        
        async with self.semaphore:
            response = await self.client.post(url, headers=self.headers, json=records)
            if response.status_code not in [200, 201]:
                raise Exception(f"Bulk insert failed: {response.text}")
            return response.json()
