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
        # V1 tables listing endpoint
        endpoint = f"{url.rstrip('/')}/api/v1/db/meta/projects/{base_id}/tables"
        res = await client.get(endpoint, headers=headers)
        if res.status_code == 200:
            tables = res.json().get('list', [])
            for t in tables:
                if 'columns' in t:
                    print(f"Table {t['id']} has {len(t['columns'])} columns via V1 tables endpoint!")
                else:
                    print(f"Table {t['id']} NO COLUMNS array.")
            print(f"Total tables: {len(tables)}")
        
        # Test V2 tables endpoint
        endpoint2 = f"{url.rstrip('/')}/api/v2/meta/bases/{base_id}/tables"
        res2 = await client.get(endpoint2, headers=headers)
        if res2.status_code == 200:
            tables = res2.json().get('list', [])
            for t in tables:
                if 'columns' in t:
                    print(f"Table {t['id']} has {len(t['columns'])} columns via V2 tables endpoint!")

asyncio.run(main())
