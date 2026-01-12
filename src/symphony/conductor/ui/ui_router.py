from pathlib import Path
from fastapi import APIRouter
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

router_ui = APIRouter(prefix="/ui", tags=["ui"])


DIST_DIR = Path(__file__).parent / "dist"
INDEX_HTML = DIST_DIR / "index.html"

router_ui.mount("/ui", StaticFiles(directory=DIST_DIR, html=False), name="dashboard-static")

@router_ui.get("/", include_in_schema=False)
def dashboard_root():
    return FileResponse(INDEX_HTML)

@router_ui.get("/{full_path:path}", include_in_schema=False)
def dashboard_spa(full_path: str):
    file_path = DIST_DIR / full_path
    if file_path.is_file():
        return FileResponse(file_path)

    return FileResponse(INDEX_HTML)
