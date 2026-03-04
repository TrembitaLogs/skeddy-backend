import re
from typing import Literal

from pydantic import BaseModel, field_validator


class VersionUpdateRequest(BaseModel):
    """Request schema for updating minimum app version."""

    version: str

    @field_validator("version")
    @classmethod
    def validate_semver(cls, v: str) -> str:
        if not re.match(r"^\d+\.\d+\.\d+$", v):
            raise ValueError("Version must be in semver format (e.g. 1.2.3)")
        return v


class VersionResponse(BaseModel):
    """Response schema for minimum app version."""

    min_search_app_version: str


class BackupStatusResponse(BaseModel):
    """Response schema for backup status."""

    status: str
    timestamp: str | None = None
    file: str | None = None
    size: int | None = None
    duration: int | None = None
    error: str | None = None
    local_backup_count: int = 0
    local_backup_total_size: int = 0


class BackupSettingsResponse(BaseModel):
    """Response schema for backup settings."""

    interval_hours: int = 24
    retention_days: int = 7


class BackupSettingsUpdate(BaseModel):
    """Request schema for updating backup settings."""

    interval_hours: Literal[1, 6, 12, 24] = 24
    retention_days: Literal[3, 7, 14, 30] = 7
