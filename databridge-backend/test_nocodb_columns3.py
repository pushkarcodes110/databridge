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
        endpoints = [
            f"/api/v2/meta/tables/{table_id}/columns",
            f"/api/v1/meta/tables/{table_id}/columns",
            f"/api/v2/meta/bases/{base_id}/tables/{table_id}/columns",
            f"/api/v1/db/meta/projects/{base_id}/tables/{table_id}/columns", # legacy
            f"/api/v2/meta/columns",  # maybe global?
            f"/api/v2/meta/tables/{table_id}", # Does table definition have columns?
        ]
        for ep in endpoints:
            res = await client.get(f"{url.rstrip('/')}{ep}", headers=headers)
            print(f"GET {ep} -> {res.status_code}")
            if res.status_code == 200:
                print("SUCCESS!")
                print(str(res.json().keys()) if isinstance(res.json(), dict) else "List")

asyncio.run(main())
