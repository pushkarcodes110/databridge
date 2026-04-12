import os
import pandas as pd
from typing import List, Dict, Any

from app.config import settings

def get_file_preview(file_id: str, filename: str, rows: int = 20) -> Dict[str, Any]:
    """
    Reads the first `rows` of a file to return the headers and sample data
    for the frontend column mapping UI.
    """
    safe_filename = f"{file_id}_{filename}"
    file_path = os.path.join(settings.upload_dir, safe_filename)
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File {safe_filename} not found.")

    ext = os.path.splitext(filename)[1].lower()
    
    df = None
    if ext == ".csv":
        # read only the first `rows` efficiently
        df = pd.read_csv(file_path, nrows=rows)
    elif ext in [".xlsx", ".xls"]:
        df = pd.read_excel(file_path, nrows=rows)
    elif ext == ".json":
        df = pd.read_json(file_path)
        if len(df) > rows:
            df = df.head(rows)
    else:
        raise ValueError(f"Unsupported file format: {ext}")
        
    # Replace NaN with None for valid JSON serialization
    df = df.fillna("")
    
    # Compute simple stats
    total_rows = 0
    unique_rows = 0
    file_size_mb = round(os.path.getsize(file_path) / (1024 * 1024), 2)
    
    try:
        if ext == ".csv":
            seen = set()
            with open(file_path, 'rb') as f:
                header = f.readline()
                for line in f:
                    total_rows += 1
                    seen.add(hash(line))
            unique_rows = len(seen)
            
        elif ext in [".xlsx", ".xls"]:
            # For excel, we might just load it if we need accurate unique counts
            # But pd.read_excel(file_path) could be risky for massive files.
            # We'll do it safely since they are usually smaller than CSVs.
            full_df = pd.read_excel(file_path)
            total_rows = len(full_df)
            
            # Simple heuristic for uniqueness: convert series to tuple
            total_dupes = int(full_df.duplicated().sum())
            unique_rows = int(total_rows - total_dupes)
            
        elif ext == ".json":
            import json
            with open(file_path, 'r') as f:
                full_data = json.load(f)
            if isinstance(full_data, list):
                total_rows = len(full_data)
                unique_rows = len(set(str(d) for d in full_data))
    except Exception as e:
        print(f"Error calculating stats: {e}")
        # fallback if computation fails
        pass
    
    columns = df.columns.tolist()
    
    # Convert dataframe to json-safe dicts (handles numpy types natively)
    import json
    data = json.loads(df.to_json(orient="records"))
    
    return {
        "columns": columns,
        "rows": data,
        "stats": {
            "total_rows": int(total_rows),
            "unique_rows": int(unique_rows),
            "file_size_mb": file_size_mb
        }
    }
