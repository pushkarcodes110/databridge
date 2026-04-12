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
table_id = "mp4kyfgbnkfqhb6" # the one user just created

async def main():
    async with httpx.AsyncClient() as client:
        # 1. Fetch tables under the base
        res = await client.get(f"{url.rstrip('/')}/api/v2/meta/bases/{base_id}/tables", headers=headers)
        if res.status_code == 200:
            tables = res.json().get('list', [])
            for t in tables:
                if t['id'] == table_id:
                    print("Found table!")
                    if 'columns' in t: print(f"Columns exist in table list! count: {len(t['columns'])}")
                    else: print("No columns in table listing.")

        # 2. Test possible endpoints for this table
        endpoints = [
            f"/api/v1/db/meta/projects/{base_id}/tables/{table_id}/columns",
            f"/api/v2/meta/tables/{table_id}/columns",
            f"/api/v2/meta/bases/{base_id}/tables/{table_id}",
            f"/api/v1/meta/tables/{table_id}/columns",
            f"/api/v1/db/meta/columns",
            f"/api/v1/db/meta/tables/{table_id}",
            f"/api/v1/db/meta/projects/tables/{table_id}/columns"
        ]
        for ep in endpoints:
            res = await client.get(f"{url.rstrip('/')}{ep}", headers=headers)
            print(f"GET {ep} -> {res.status_code}")

asyncio.run(main())
