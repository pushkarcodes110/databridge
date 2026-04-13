from sqlalchemy import create_engine, text
import pandas as pd

engine = create_engine("postgresql://user:pushkar@localhost:5432/databridge")
with engine.connect() as conn:
    res = conn.execute(text("SELECT id, status, error_summary, nocodb_table_id FROM import_jobs ORDER BY created_at DESC LIMIT 5"))
    for row in res:
        print(f"ID: {row[0]} | Status: {row[1]} | Table: {row[3]}")
        print(f"Error: {row[2]}")
        print("-" * 20)
