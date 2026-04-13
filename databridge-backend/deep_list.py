import asyncio
import httpx
from sqlalchemy import create_engine, text
import json

engine = create_engine("postgresql://user:pushkar@localhost:5432/databridge")
with engine.connect() as conn:
    res = conn.execute(text("SELECT nocodb_url, nocodb_api_token FROM settings LIMIT 1")).fetchone()
    url, token = res

headers = {"xc-token": token}
base_id = "p5w60c6wwg73y7l"

async def main():
    async with httpx.AsyncClient() as client:
        res = await client.get(f"{url.rstrip('/')}/api/v1/db/meta/projects/{base_id}/tables", headers=headers)
        if res.status_code == 200:
            data = res.json()
            tables = data.get('list', [])
            for t in tables:
                print(f"Table {t.get('title')} ({t.get('id')})")
                cols = t.get('columns')
                if cols:
                    print(f"  HAS COLUMNS: {len(cols)}")
                else:
                    print("  NO COLUMNS IN LISTING")
                    # Try to get single table via the EXACT path from some docs
                    single_res = await client.get(f"{url.rstrip('/')}/api/v1/db/meta/projects/{base_id}/tables/{t.get('id')}", headers=headers)
                    print(f"  GET single -> {single_res.status_code}")
                    if single_res.status_code == 200:
                         print("  SINGLE SUCCESS! Keys:", single_res.json().keys())

asyncio.run(main())
