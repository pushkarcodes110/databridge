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

async def main():
    async with httpx.AsyncClient() as client:
        res = await client.get(f"{url.rstrip('/')}/api/v1/db/meta/projects/{base_id}/tables", headers=headers)
        print("V1 tables:")
        if res.status_code == 200:
            for t in res.json().get('list', []):
                print(t['id'], t['title'])
        
        res2 = await client.get(f"{url.rstrip('/')}/api/v2/meta/bases/{base_id}/tables", headers=headers)
        print("\nV2 tables:")
        if res2.status_code == 200:
            for t in res2.json().get('list', []):
                print(t['id'], t['title'])

asyncio.run(main())
