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
        res = await client.get(f"{url.rstrip('/')}/api/v2/meta/bases/{base_id}/tables", headers=headers)
        if res.status_code == 200:
            tables = res.json().get('list', [])
            if len(tables) > 0:
                print("Table keys:", tables[0].keys())
                if 'columns' in tables[0]:
                    print("COLUMNS EXIST!")
                    print(len(tables[0]['columns']))
                else:
                    print("No columns array.")
        else:
            print("Failed:", res.status_code)

asyncio.run(main())
