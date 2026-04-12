import asyncio
import httpx
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
import json
import uuid

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
            "title": f"Test_Col_{uuid.uuid4().hex[:5]}",
            "table_name": f"test_col_{uuid.uuid4().hex[:5]}",
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
        res = await client.post(f"{url.rstrip('/')}/api/v2/meta/bases/{base_id}/tables", headers=headers, json=payload)
        print("STATUS:", res.status_code)
        if res.status_code == 200:
            data = res.json()
            if 'columns' in data:
                print("COLUMNS ARRAY FOUND IN CREATION RESPONSE!")
                print(json.dumps(data['columns']))
            else:
                print("KEYS:", data.keys())

asyncio.run(main())
