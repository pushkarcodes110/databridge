import asyncio
import httpx
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
import json

engine = create_engine("postgresql://user:pushkar@localhost:5432/databridge")
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
session = SessionLocal()
result = session.execute(text("SELECT nocodb_url, nocodb_api_token FROM settings LIMIT 1")).fetchone()
url, token = result
headers = {"xc-token": token}
base_id = "p5w60c6wwg73y7l"
table_id = "mp4kyfgbnkfqhb6"

async def main():
    async with httpx.AsyncClient() as client:
        endpoints = [
            f"/api/v2/meta/tables/{table_id}/columns",
            f"/api/v2/meta/bases/{base_id}/tables/{table_id}/columns",
            f"/api/v1/meta/tables/{table_id}",
            f"/api/v1/db/meta/tables/{table_id}",
            f"/api/v2/meta/columns?tableId={table_id}",
            f"/api/v1/db/meta/columns?tableId={table_id}",
            f"/api/v1/db/meta/projects/{base_id}/tables/{table_id}",
            f"/api/v2/meta/bases/{base_id}/tables/{table_id}",
            f"/api/v2/meta/tables/{table_id}",
            f"/api/v1/db/data/noco/{base_id}/{table_id}/views",
            f"/api/v1/db/meta/projects/{base_id}/tables",
        ]
        for ep in endpoints:
            res = await client.get(f"{url.rstrip('/')}{ep}", headers=headers)
            print(f"GET {ep} -> {res.status_code}")
            if res.status_code == 200:
                data = res.json()
                if isinstance(data, dict) and 'columns' in data:
                    print(f"FOUND COLUMNS IN {ep}!")
                    break
                elif isinstance(data, dict) and 'list' in data and len(data['list']) > 0 and 'columns' in data['list'][0]:
                    print(f"FOUND COLUMNS IN LIST IN {ep}!")
                    break
        
        # Test fetching a record to infer schema
        data_ep = f"/api/v1/db/data/noco/{base_id}/{table_id}?limit=1"
        res = await client.get(f"{url.rstrip('/')}{data_ep}", headers=headers)
        if res.status_code == 200:
            print("Data API works!")
            print(res.json())

asyncio.run(main())
