"""Admin API endpoints for database backup management."""

import json
import logging
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request

from app.config import settings
from app.schemas.admin import (
    BackupSettingsResponse,
    BackupSettingsUpdate,
    BackupStatusResponse,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/admin/backup", tags=["admin-backup"])

BACKUP_DIR = Path(settings.BACKUP_DIR)
CONFIG_FILE = BACKUP_DIR / ".backup_config.json"
STATUS_FILE = BACKUP_DIR / ".last_backup_status"
TRIGGER_FILE = BACKUP_DIR / ".trigger_backup"


async def require_admin(request: Request) -> None:
    """Dependency that checks for an authenticated admin session."""
    if not request.session.get("admin_authenticated"):
        raise HTTPException(status_code=401, detail="NOT_AUTHENTICATED")


def _read_json_file(path: Path) -> dict[str, Any]:
    """Read and parse a JSON file, return empty dict on failure."""
    try:
        result: dict[str, Any] = json.loads(path.read_text())
        return result
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _get_local_backup_stats() -> tuple[int, int]:
    """Return (count, total_size_bytes) of local .dump backup files."""
    count = 0
    total_size = 0
    if BACKUP_DIR.exists():
        for f in BACKUP_DIR.glob("skeddy_backup_*.dump"):
            count += 1
            total_size += f.stat().st_size
    return count, total_size


@router.get(
    "/status",
    response_model=BackupStatusResponse,
    dependencies=[Depends(require_admin)],
)
async def get_backup_status() -> BackupStatusResponse:
    """Return last backup status and local backup statistics."""
    data = _read_json_file(STATUS_FILE)
    count, total_size = _get_local_backup_stats()
    return BackupStatusResponse(
        status=data.get("status", "unknown"),
        timestamp=data.get("timestamp"),
        file=data.get("file"),
        size=data.get("size"),
        duration=data.get("duration"),
        error=data.get("error"),
        local_backup_count=count,
        local_backup_total_size=total_size,
    )


@router.post(
    "/trigger",
    dependencies=[Depends(require_admin)],
)
async def trigger_backup() -> dict:
    """Create trigger file to initiate a manual backup on next cron cycle."""
    try:
        BACKUP_DIR.mkdir(parents=True, exist_ok=True)
        TRIGGER_FILE.touch()
        logger.info("Manual backup triggered via admin API")
        return {"message": "Backup triggered. It will start within 30 minutes."}
    except OSError as e:
        raise HTTPException(status_code=500, detail=f"Failed to trigger backup: {e}")


@router.get(
    "/settings",
    response_model=BackupSettingsResponse,
    dependencies=[Depends(require_admin)],
)
async def get_backup_settings() -> BackupSettingsResponse:
    """Return current backup settings."""
    data = _read_json_file(CONFIG_FILE)
    return BackupSettingsResponse(
        interval_hours=data.get("interval_hours", 24),
        retention_days=data.get("retention_days", 7),
    )


@router.put(
    "/settings",
    response_model=BackupSettingsResponse,
    dependencies=[Depends(require_admin)],
)
async def update_backup_settings(body: BackupSettingsUpdate) -> BackupSettingsResponse:
    """Update backup settings (interval and retention)."""
    try:
        BACKUP_DIR.mkdir(parents=True, exist_ok=True)
        config = {"interval_hours": body.interval_hours, "retention_days": body.retention_days}
        CONFIG_FILE.write_text(json.dumps(config, indent=2))
        logger.info(
            "Backup settings updated: interval=%dh retention=%dd",
            body.interval_hours,
            body.retention_days,
        )
        return BackupSettingsResponse(**config)
    except OSError as e:
        raise HTTPException(status_code=500, detail=f"Failed to save settings: {e}")
