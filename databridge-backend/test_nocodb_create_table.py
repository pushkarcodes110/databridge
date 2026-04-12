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
headers = {"xc-token": token, "Content-Type": "application/json"}
base_id = "p5w60c6wwg73y7l"

async def main():
    async with httpx.AsyncClient() as client:
        payload = {
            "title": "Test Creation Table",
            "table_name": "test_creation_table",
            "columns": [
                {
                    "column_name": "id",
                    "title": "Id",
                    "uidt": "ID",
                    "pk": True,
                    "ai": True
                },
                {
                    "column_name": "name",
                    "title": "Name",
                    "uidt": "SingleLineText"
                }
            ]
        }
        
        # Try v2 first
        ep = f"/api/v2/meta/bases/{base_id}/tables"
        res = await client.post(f"{url.rstrip('/')}{ep}", headers=headers, json=payload)
        print(f"POST {ep} -> {res.status_code}")
        if res.status_code in [200, 201]:
            print(res.json().get('id'))
        else:
            print(res.text)

asyncio.run(main())
