import asyncio
import httpx
from sqlalchemy import create_engine, text

engine = create_engine("postgresql://user:pushkar@localhost:5432/databridge")
with engine.connect() as conn:
    res = conn.execute(text("SELECT nocodb_url, nocodb_api_token FROM settings LIMIT 1")).fetchone()
    url, token = res

headers = {"xc-token": token}
base_id = "p5w60c6wwg73y7l"
table_id = "mji3s8b9871pl9x"
ws_id = "wln223ks"

async def test_ep(client, ep):
    res = await client.get(f"{url.rstrip('/')}{ep}", headers=headers)
    if res.status_code == 200:
        print(f"SUCCESS: {ep}")
        return True
    return False

async def main():
    async with httpx.AsyncClient() as client:
        # Check if we can get table details via workspace
        await test_ep(client, f"/api/v1/workspaces/{ws_id}/bases/{base_id}/tables/{table_id}")
        await test_ep(client, f"/api/v1/db/meta/tables/{table_id}")
        
        # Try a v2 meta fetch for base tables with columns
        res = await client.get(f"{url.rstrip('/')}/api/v2/meta/bases/{base_id}/tables", headers=headers)
        if res.status_code == 200:
            tables = res.json().get('list', [])
            for t in tables:
                if t['id'] == table_id:
                    print(f"Found table {table_id} in V2 list")
                    print("Keys:", t.keys())
                    if 'columns' in t: print("COLUMNS ARE HERE!")
        
        # Try to get the columns via the 'columns' meta endpoint with a query param
        await test_ep(client, f"/api/v1/db/meta/columns?fk_model_id={table_id}")
        await test_ep(client, f"/api/v1/db/meta/columns?tableId={table_id}")

asyncio.run(main())
