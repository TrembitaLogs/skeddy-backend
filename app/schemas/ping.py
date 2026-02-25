from datetime import datetime

from pydantic import BaseModel, Field


class AcceptFailureItem(BaseModel):
    """Single accept failure entry reported by search device."""

    reason: str
    ride_price: float | None = None
    pickup_time: str | None = None
    timestamp: datetime


class DeviceHealth(BaseModel):
    """Health status of the search device."""

    accessibility_enabled: bool | None = None
    lyft_running: bool | None = None
    screen_on: bool | None = None


class PingStats(BaseModel):
    """Batch statistics reported by search device."""

    batch_id: str
    cycles_since_last_ping: int = Field(ge=0)
    rides_found: int = Field(ge=0)
    accept_failures: list[AcceptFailureItem] = []


class RideStatusReport(BaseModel):
    """Single ride verification status reported by search device."""

    ride_hash: str = Field(min_length=1)
    present: bool


class PingRequest(BaseModel):
    """Request schema for POST /ping from search device."""

    timezone: str = Field(min_length=1)
    app_version: str = Field(min_length=1)
    device_health: DeviceHealth | None = None
    stats: PingStats | None = None
    last_cycle_duration_ms: int | None = None
    ride_statuses: list[RideStatusReport] | None = None


class PingFiltersResponse(BaseModel):
    """Minimal filters included in ping response (only fields needed by search device)."""

    min_price: float


class VerifyRideItem(BaseModel):
    """Single ride hash for Search App to verify presence in Lyft Driver."""

    ride_hash: str


class PingResponse(BaseModel):
    """Response schema for POST /ping."""

    search: bool
    interval_seconds: int
    force_update: bool = False
    update_url: str | None = None
    reason: str | None = None
    filters: PingFiltersResponse
    verify_rides: list[VerifyRideItem] | None = None
