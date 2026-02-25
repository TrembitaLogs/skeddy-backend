from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError

from app.schemas.search import (
    DeviceOverrideRequest,
    SearchStatusResponse,
    calculate_is_online,
)


class TestSearchStatusResponse:
    """SearchStatusResponse schema validation."""

    def test_all_fields_false_and_none(self):
        """Schema accepts is_active=False, is_online=False, last_ping_at=None."""
        response = SearchStatusResponse(
            is_active=False,
            is_online=False,
            last_ping_at=None,
            credits_balance=0,
        )
        assert response.is_active is False
        assert response.is_online is False
        assert response.last_ping_at is None
        assert response.credits_balance == 0

    def test_all_fields_populated(self):
        """Schema accepts fully populated fields."""
        ts = datetime(2026, 2, 9, 14, 58, 30, tzinfo=UTC)
        response = SearchStatusResponse(
            is_active=True,
            is_online=True,
            last_ping_at=ts,
            credits_balance=42,
        )
        assert response.is_active is True
        assert response.is_online is True
        assert response.last_ping_at == ts
        assert response.credits_balance == 42

    def test_serialization_datetime_as_string(self):
        """last_ping_at serializes to string in JSON mode."""
        ts = datetime(2026, 2, 9, 14, 58, 30, tzinfo=UTC)
        response = SearchStatusResponse(
            is_active=True,
            is_online=True,
            last_ping_at=ts,
            credits_balance=10,
        )
        data = response.model_dump(mode="json")
        assert isinstance(data["last_ping_at"], str)

    def test_missing_required_field_raises(self):
        """Missing is_active raises ValidationError."""
        with pytest.raises(ValidationError):
            SearchStatusResponse(is_online=False, last_ping_at=None, credits_balance=0)

    def test_missing_credits_balance_raises(self):
        """Missing credits_balance raises ValidationError."""
        with pytest.raises(ValidationError):
            SearchStatusResponse(is_active=False, is_online=False, last_ping_at=None)


class TestCalculateIsOnline:
    """calculate_is_online helper function."""

    def test_recent_ping_returns_true(self):
        """Ping 5s ago with interval=30 → True (5 < 60)."""
        last_ping = datetime.now(UTC) - timedelta(seconds=5)
        assert calculate_is_online(last_ping, interval=30) is True

    def test_stale_ping_returns_false(self):
        """Ping 65s ago with interval=30 → False (65 > 60)."""
        last_ping = datetime.now(UTC) - timedelta(seconds=65)
        assert calculate_is_online(last_ping, interval=30) is False

    def test_none_ping_returns_false(self):
        """last_ping_at=None → False (device never pinged)."""
        assert calculate_is_online(None, interval=30) is False

    def test_exactly_at_boundary_returns_false(self):
        """Ping exactly at interval*2 boundary → False (not strictly less)."""
        last_ping = datetime.now(UTC) - timedelta(seconds=60)
        assert calculate_is_online(last_ping, interval=30) is False

    def test_just_within_boundary_returns_true(self):
        """Ping 59s ago with interval=30 → True (59 < 60)."""
        last_ping = datetime.now(UTC) - timedelta(seconds=59)
        assert calculate_is_online(last_ping, interval=30) is True

    def test_custom_interval(self):
        """Works with non-default interval values."""
        last_ping = datetime.now(UTC) - timedelta(seconds=100)
        assert calculate_is_online(last_ping, interval=60) is True  # 100 < 120
        assert calculate_is_online(last_ping, interval=45) is False  # 100 > 90


class TestDeviceOverrideRequest:
    """DeviceOverrideRequest schema validation."""

    def test_active_true(self):
        """active=True is accepted."""
        request = DeviceOverrideRequest(active=True)
        assert request.active is True

    def test_active_false(self):
        """active=False is accepted."""
        request = DeviceOverrideRequest(active=False)
        assert request.active is False

    def test_missing_active_raises(self):
        """Missing active field raises ValidationError."""
        with pytest.raises(ValidationError):
            DeviceOverrideRequest()
