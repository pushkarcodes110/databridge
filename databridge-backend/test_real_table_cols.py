import asyncio
import httpx
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text

engine = create_engine("postgresql://user:pushkar@localhost:5432/databridge")
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
session = SessionLocal()
result = session.execute(text("SELECT nocodb_url, nocodb_api_token FROM settings LIMIT 1")).fetchone()
url, token = result
headers = {"xc-token": token}
base_id = "p5w60c6wwg73y7l"
table_id = "mji3s8b9871pl9x" # 100% real table ID

async def main():
    async with httpx.AsyncClient() as client:
        endpoints = [
            f"/api/v2/meta/tables/{table_id}",
            f"/api/v1/db/meta/tables/{table_id}",
            f"/api/v2/meta/tables/{table_id}/columns",
            f"/api/v1/meta/tables/{table_id}/columns"
        ]
        
        for ep in endpoints:
            res = await client.get(f"{url.rstrip('/')}{ep}", headers=headers)
            print(f"GET {ep} -> {res.status_code}")
            if res.status_code == 200:
                data = res.json()
                if isinstance(data, dict):
                    print("Keys:", data.keys())
                    if 'columns' in data:
                        print("We found columns directly in table object!")
                elif isinstance(data, list):
                    print("List of len", len(data))
                    if len(data) > 0:
                        print("First item keys:", data[0].keys())

asyncio.run(main())
