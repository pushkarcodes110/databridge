import os
import aiofiles
from fastapi import APIRouter, File, UploadFile, Form, HTTPException, status
from pydantic import BaseModel
from typing import Optional

from app.config import settings

router = APIRouter(prefix="/api/upload", tags=["Upload"])

os.makedirs(settings.upload_dir, exist_ok=True)

class UploadResponse(BaseModel):
    message: str
    file_id: Optional[str] = None
    file_path: Optional[str] = None
    progress: Optional[float] = None

@router.post("/chunk", response_model=UploadResponse)
async def upload_chunk(
    file: UploadFile = File(...),
    chunk_index: int = Form(...),
    total_chunks: int = Form(...),
    file_id: str = Form(...)  # frontend generates a unique ID per file
):
    """
    Handle chunked file upload to bypass size limits.
    """
    # Create absolute path safely
    safe_filename = f"{file_id}_{file.filename}"
    file_path = os.path.join(settings.upload_dir, safe_filename)

    # Mode: if it's the first chunk, overwrite/create; else append ('ab')
    mode = "ab" if chunk_index > 0 else "wb"
    
    try:
        async with aiofiles.open(file_path, mode) as f:
            content = await file.read()
            await f.write(content)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to write chunk: {str(e)}")

    if chunk_index == total_chunks - 1:
        # File upload completed
        return UploadResponse(
            message="Upload complete",
            file_id=file_id,
            file_path=file_path,
            progress=100.0
        )
    
    # Still uploading
    progress = round(((chunk_index + 1) / total_chunks) * 100, 2)
    return UploadResponse(
        message="Chunk received",
        progress=progress
    )

from app.services.parser import get_file_preview
from typing import Any, Dict

@router.get("/{file_id}/preview", response_model=Dict[str, Any])
async def upload_preview(file_id: str, filename: str):
    """
    Return headers and first 20 rows of uploaded file.
    """
    try:
        preview_data = get_file_preview(file_id, filename)
        return preview_data
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

