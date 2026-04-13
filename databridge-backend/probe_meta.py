import asyncio
import httpx
from sqlalchemy import create_engine, text

engine = create_engine("postgresql://user:pushkar@localhost:5432/databridge")
with engine.connect() as conn:
    res = conn.execute(text("SELECT nocodb_url, nocodb_api_token FROM settings LIMIT 1")).fetchone()
    url, token = res

headers = {"xc-token": token}
base_id = "p5w60c6wwg73y7l"
table_id = "mji3s8b9871pl9x" # the banking one

async def test_ep(client, ep):
    res = await client.get(f"{url.rstrip('/')}{ep}", headers=headers)
    print(f"GET {ep} -> {res.status_code}")
    if res.status_code == 200:
        print("KEYS:", res.json().keys() if isinstance(res.json(), dict) else "LIST")

async def main():
    async with httpx.AsyncClient() as client:
        eps = [
            f"/api/v1/db/meta/tables/{table_id}",
            f"/api/v1/meta/tables/{table_id}",
            f"/api/v2/meta/tables/{table_id}",
            f"/api/v1/db/meta/models/{table_id}",
            f"/api/v1/meta/models/{table_id}",
            f"/api/v1/db/meta/projects/{base_id}/tables/{table_id}",
        ]
        for ep in eps:
            await test_ep(client, ep)

asyncio.run(main())
