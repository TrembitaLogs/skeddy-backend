"""Admin panel view for database backup management."""

import json
from pathlib import Path
from typing import Any

from sqladmin import BaseView, expose

from app.config import settings

BACKUP_DIR = Path(settings.BACKUP_DIR)
CONFIG_FILE = BACKUP_DIR / ".backup_config.json"
STATUS_FILE = BACKUP_DIR / ".last_backup_status"


def _read_json_file(path: Path) -> dict[str, Any]:
    """Read and parse a JSON file, return empty dict on failure."""
    try:
        result: dict[str, Any] = json.loads(path.read_text())
        return result
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _format_size(size_bytes: int) -> str:
    """Format bytes into human-readable string."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    if size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    return f"{size_bytes / (1024 * 1024):.1f} MB"


def _format_timestamp(ts: str | None) -> str:
    """Convert YYYYMMDD_HHMMSS to readable format."""
    if not ts or len(ts) < 15:
        return "Never"
    return f"{ts[:4]}-{ts[4:6]}-{ts[6:8]} {ts[9:11]}:{ts[11:13]}:{ts[13:15]} UTC"


class BackupView(BaseView):
    """Backup management view in admin panel."""

    name = "Backups"
    icon = "fa-solid fa-database"

    @expose("/backup", methods=["GET"])
    async def backup_page(self, request):
        """Render backup management page."""
        # Last backup status
        status_data = _read_json_file(STATUS_FILE)
        last_status = status_data.get("status", "unknown")
        last_timestamp = _format_timestamp(status_data.get("timestamp"))
        last_file = status_data.get("file", "-")
        last_size = _format_size(status_data.get("size", 0))
        last_duration = status_data.get("duration", 0)
        last_error = status_data.get("error", "")

        # Current settings
        config_data = _read_json_file(CONFIG_FILE)
        interval_hours = config_data.get("interval_hours", 24)
        retention_days = config_data.get("retention_days", 7)

        # Local backup stats
        backup_count = 0
        total_size = 0
        if BACKUP_DIR.exists():
            for f in BACKUP_DIR.glob("skeddy_backup_*.dump"):
                backup_count += 1
                total_size += f.stat().st_size

        return await self.templates.TemplateResponse(
            request,
            "admin/backup.html",
            {
                "last_status": last_status,
                "last_timestamp": last_timestamp,
                "last_file": last_file,
                "last_size": last_size,
                "last_duration": last_duration,
                "last_error": last_error,
                "interval_hours": interval_hours,
                "retention_days": retention_days,
                "backup_count": backup_count,
                "total_size": _format_size(total_size),
            },
        )
