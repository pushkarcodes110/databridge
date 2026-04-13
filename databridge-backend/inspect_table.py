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
            tables = res.json().get('list', [])
            if len(tables) > 0:
                print("KEYS of first table:")
                print(tables[0].keys())
                # print the whole first table object
                print(json.dumps(tables[0], indent=2))

asyncio.run(main())
