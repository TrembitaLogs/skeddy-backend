"""Tests for cursor-based pagination utilities."""

import uuid
from datetime import UTC, datetime, timedelta, timezone

import pytest

from app.utils.pagination import decode_cursor, encode_cursor

# -- Fixtures ----------------------------------------------------------------

SAMPLE_UUID = uuid.UUID("550e8400-e29b-41d4-a716-446655440000")
SAMPLE_UUID_2 = uuid.UUID("a1b2c3d4-e5f6-7890-abcd-ef1234567890")


# -- encode_cursor -----------------------------------------------------------


class TestEncodeCursor:
    """Tests for encode_cursor function."""

    def test_encode_ride_cursor(self):
        dt = datetime(2026, 2, 21, 10, 30, 0, tzinfo=UTC)
        result = encode_cursor(dt, "ride", SAMPLE_UUID)
        assert result == f"2026-02-21T10:30:00Z_ride_{SAMPLE_UUID}"

    def test_encode_credit_cursor(self):
        dt = datetime(2026, 2, 20, 8, 0, 0, tzinfo=UTC)
        result = encode_cursor(dt, "credit", SAMPLE_UUID_2)
        assert result == f"2026-02-20T08:00:00Z_credit_{SAMPLE_UUID_2}"

    def test_encode_preserves_microseconds(self):
        dt = datetime(2026, 2, 21, 10, 30, 0, 123456, tzinfo=UTC)
        result = encode_cursor(dt, "ride", SAMPLE_UUID)
        assert result == f"2026-02-21T10:30:00.123456Z_ride_{SAMPLE_UUID}"

    def test_encode_omits_microseconds_when_zero(self):
        dt = datetime(2026, 2, 21, 10, 30, 0, 0, tzinfo=UTC)
        result = encode_cursor(dt, "ride", SAMPLE_UUID)
        assert result == f"2026-02-21T10:30:00Z_ride_{SAMPLE_UUID}"

    def test_encode_normalizes_to_utc(self):
        est = timezone(timedelta(hours=-5))
        dt = datetime(2026, 2, 21, 5, 30, 0, tzinfo=est)  # 05:30 EST = 10:30 UTC
        result = encode_cursor(dt, "ride", SAMPLE_UUID)
        assert result.startswith("2026-02-21T10:30:00Z_")

    def test_encode_invalid_event_kind(self):
        dt = datetime(2026, 2, 21, 10, 30, 0, tzinfo=UTC)
        with pytest.raises(ValueError, match="Invalid event_kind"):
            encode_cursor(dt, "invalid", SAMPLE_UUID)

    def test_encode_naive_datetime_raises(self):
        dt = datetime(2026, 2, 21, 10, 30, 0)
        with pytest.raises(ValueError, match="timezone-aware"):
            encode_cursor(dt, "ride", SAMPLE_UUID)


# -- decode_cursor -----------------------------------------------------------


class TestDecodeCursor:
    """Tests for decode_cursor function."""

    def test_decode_valid_ride_cursor(self):
        cursor = f"2026-02-21T10:30:00Z_ride_{SAMPLE_UUID}"
        dt, kind, event_id = decode_cursor(cursor)
        assert dt == datetime(2026, 2, 21, 10, 30, 0, tzinfo=UTC)
        assert kind == "ride"
        assert event_id == SAMPLE_UUID

    def test_decode_valid_credit_cursor(self):
        cursor = f"2026-02-20T08:00:00Z_credit_{SAMPLE_UUID_2}"
        dt, kind, event_id = decode_cursor(cursor)
        assert dt == datetime(2026, 2, 20, 8, 0, 0, tzinfo=UTC)
        assert kind == "credit"
        assert event_id == SAMPLE_UUID_2

    def test_decode_with_microseconds(self):
        cursor = f"2026-02-21T10:30:00.123456Z_ride_{SAMPLE_UUID}"
        dt, kind, _event_id = decode_cursor(cursor)
        assert dt == datetime(2026, 2, 21, 10, 30, 0, 123456, tzinfo=UTC)
        assert kind == "ride"

    def test_decode_invalid_format_no_separators(self):
        with pytest.raises(ValueError, match="Invalid cursor format"):
            decode_cursor("no-underscores-here")

    def test_decode_invalid_format_too_few_parts(self):
        with pytest.raises(ValueError, match="Invalid cursor format"):
            decode_cursor("2026-02-21T10:30:00Z_ride")

    def test_decode_invalid_event_kind(self):
        with pytest.raises(ValueError, match="Invalid event_kind"):
            decode_cursor(f"2026-02-21T10:30:00Z_purchase_{SAMPLE_UUID}")

    def test_decode_no_z_suffix(self):
        with pytest.raises(ValueError, match="must end with 'Z'"):
            decode_cursor(f"2026-02-21T10:30:00+00:00_ride_{SAMPLE_UUID}")

    def test_decode_malformed_datetime(self):
        with pytest.raises(ValueError, match="Invalid datetime"):
            decode_cursor(f"not-a-dateZ_ride_{SAMPLE_UUID}")

    def test_decode_invalid_uuid(self):
        with pytest.raises(ValueError, match="Invalid UUID"):
            decode_cursor("2026-02-21T10:30:00Z_ride_not-a-valid-uuid")

    def test_decode_empty_string(self):
        with pytest.raises(ValueError):
            decode_cursor("")


# -- Roundtrip ---------------------------------------------------------------


class TestCursorRoundtrip:
    """encode -> decode returns original values."""

    def test_roundtrip_ride(self):
        original_dt = datetime(2026, 2, 21, 10, 30, 0, tzinfo=UTC)
        original_id = uuid.uuid4()
        cursor = encode_cursor(original_dt, "ride", original_id)
        decoded_dt, decoded_kind, decoded_id = decode_cursor(cursor)
        assert decoded_dt == original_dt
        assert decoded_kind == "ride"
        assert decoded_id == original_id

    def test_roundtrip_credit(self):
        original_dt = datetime(2026, 2, 20, 8, 0, 0, tzinfo=UTC)
        original_id = uuid.uuid4()
        cursor = encode_cursor(original_dt, "credit", original_id)
        decoded_dt, decoded_kind, decoded_id = decode_cursor(cursor)
        assert decoded_dt == original_dt
        assert decoded_kind == "credit"
        assert decoded_id == original_id

    def test_roundtrip_with_microseconds(self):
        original_dt = datetime(2026, 2, 21, 10, 30, 0, 123456, tzinfo=UTC)
        original_id = uuid.uuid4()
        cursor = encode_cursor(original_dt, "ride", original_id)
        decoded_dt, _, decoded_id = decode_cursor(cursor)
        assert decoded_dt == original_dt
        assert decoded_id == original_id

    def test_roundtrip_normalizes_timezone(self):
        est = timezone(timedelta(hours=-5))
        original_dt = datetime(2026, 2, 21, 5, 30, 0, tzinfo=est)
        original_id = uuid.uuid4()
        cursor = encode_cursor(original_dt, "ride", original_id)
        decoded_dt, _, decoded_id = decode_cursor(cursor)
        assert decoded_dt == original_dt.astimezone(UTC)
        assert decoded_id == original_id
