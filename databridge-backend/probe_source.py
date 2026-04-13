import asyncio
import httpx
from sqlalchemy import create_engine, text

engine = create_engine("postgresql://user:pushkar@localhost:5432/databridge")
with engine.connect() as conn:
    res = conn.execute(text("SELECT nocodb_url, nocodb_api_token FROM settings LIMIT 1")).fetchone()
    url, token = res

headers = {"xc-token": token}
base_id = "p5w60c6wwg73y7l"
source_id = "bwc7vp5a8jolf3r"
table_id = "mji3s8b9871pl9x"
table_name = "Banking_Top_leadership_data_linkdin_csv"

async def main():
    async with httpx.AsyncClient() as client:
        eps = [
            f"/api/v1/db/meta/sources/{source_id}/tables/{table_id}/columns",
            f"/api/v1/db/meta/sources/{source_id}/tables/{table_name}/columns",
            f"/api/v1/db/meta/projects/{base_id}/sources/{source_id}/tables/{table_id}/columns",
        ]
        for ep in eps:
            res = await client.get(f"{url.rstrip('/')}{ep}", headers=headers)
            print(f"GET {ep} -> {res.status_code}")

asyncio.run(main())
