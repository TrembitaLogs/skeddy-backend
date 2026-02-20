import json
from datetime import datetime
from unittest.mock import patch
from uuid import UUID
from zoneinfo import ZoneInfo

from sqlalchemy import func, select

from app.models.accept_failure import AcceptFailure
from app.models.app_config import AppConfig
from app.models.paired_device import PairedDevice
from app.models.search_filters import SearchFilters
from app.models.user import User

PING_URL = "/api/v1/ping"
REGISTER_URL = "/api/v1/auth/register"
PAIRING_GENERATE_URL = "/api/v1/pairing/generate"
PAIRING_CONFIRM_URL = "/api/v1/pairing/confirm"
SEARCH_START_URL = "/api/v1/search/start"

_TEST_PASSWORD = "securePass1"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _patch_now(target_now: datetime):
    """Patch datetime in ping_service and ping router so that datetime.now(tz) returns *target_now*.

    Replaces the datetime class inside ping_service and ping router with a thin subclass
    that overrides now() while keeping the constructor intact.
    """
    real_datetime = datetime

    class _FakeDatetime(real_datetime):
        @classmethod
        def now(cls, tz=None):
            if tz is not None:
                return target_now.astimezone(tz)
            return target_now

    import contextlib

    @contextlib.contextmanager
    def _combined():
        with (
            patch("app.services.ping_service.datetime", _FakeDatetime),
            patch("app.routers.ping.datetime", _FakeDatetime),
        ):
            yield

    return _combined()


async def _register(app_client, email="ping@example.com"):
    """Register a user and return response data with tokens."""
    resp = await app_client.post(REGISTER_URL, json={"email": email, "password": _TEST_PASSWORD})
    assert resp.status_code == 201
    return resp.json()


async def _pair_device(app_client, access_token, device_id="ping-device-001"):
    """Pair a device via generate + confirm flow."""
    gen_resp = await app_client.post(PAIRING_GENERATE_URL, headers=_jwt(access_token))
    assert gen_resp.status_code == 201
    code = gen_resp.json()["code"]

    confirm_resp = await app_client.post(
        PAIRING_CONFIRM_URL,
        json={"code": code, "device_id": device_id, "timezone": "America/New_York"},
    )
    assert confirm_resp.status_code == 200
    return confirm_resp.json()


def _jwt(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def _device_headers(device_token: str, device_id: str) -> dict:
    return {"X-Device-Token": device_token, "X-Device-Id": device_id}


def _ping_body(**overrides) -> dict:
    """Build a valid ping request body with optional overrides."""
    body = {
        "timezone": "America/New_York",
        "app_version": "1.0.0",
    }
    body.update(overrides)
    return body


async def _verify_email_in_db(db_session, user_id: str):
    """Set email_verified=True for the given user directly in DB."""
    result = await db_session.execute(select(User).where(User.id == UUID(user_id)))
    user = result.scalar_one()
    user.email_verified = True
    await db_session.commit()


async def _start_search(app_client, access_token):
    resp = await app_client.post(SEARCH_START_URL, headers=_jwt(access_token))
    assert resp.status_code == 200


async def _update_filters(db_session, user_id, **kwargs):
    """Update search filters directly in DB for the given user."""
    result = await db_session.execute(
        select(SearchFilters).where(SearchFilters.user_id == user_id)
    )
    filters = result.scalar_one()
    for key, value in kwargs.items():
        setattr(filters, key, value)
    await db_session.commit()


# ---------------------------------------------------------------------------
# Test 1: invalid device_token → 401 Unauthorized
# ---------------------------------------------------------------------------


async def test_ping_invalid_device_token_returns_401(app_client):
    """POST /ping with invalid device credentials -> 401."""
    resp = await app_client.post(
        PING_URL,
        json=_ping_body(),
        headers=_device_headers("bad-token", "bad-device"),
    )
    assert resp.status_code == 401


# ---------------------------------------------------------------------------
# Test 2: invalid timezone → 422 INVALID_TIMEZONE
# ---------------------------------------------------------------------------


async def test_ping_invalid_timezone_returns_422(app_client):
    """POST /ping with invalid IANA timezone -> 422."""
    reg = await _register(app_client, email="tz@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="tz-dev")

    resp = await app_client.post(
        PING_URL,
        json=_ping_body(timezone="Invalid/Zone"),
        headers=_device_headers(pairing["device_token"], "tz-dev"),
    )
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# Test 3: outdated app_version → search=false, force_update=true, update_url
# ---------------------------------------------------------------------------


async def test_ping_outdated_version_returns_force_update(app_client, db_session):
    """POST /ping with old app version -> force_update response.

    Also verifies device state is updated BEFORE the version check (PRD order).
    """
    reg = await _register(app_client, email="ver@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="ver-dev")

    resp = await app_client.post(
        PING_URL,
        json=_ping_body(app_version="0.0.1"),
        headers=_device_headers(pairing["device_token"], "ver-dev"),
    )

    assert resp.status_code == 200
    data = resp.json()
    assert data["search"] is False
    assert data["force_update"] is True
    assert data["update_url"] is not None
    assert data["interval_seconds"] == 300
    assert "min_price" in data["filters"]

    # Verify device state was still updated (PRD: update BEFORE version check)
    user_id = UUID(reg["user_id"])
    result = await db_session.execute(select(PairedDevice).where(PairedDevice.user_id == user_id))
    device = result.scalar_one()
    assert device.last_ping_at is not None


# ---------------------------------------------------------------------------
# Test 4: is_active=false → search=false
# ---------------------------------------------------------------------------


async def test_ping_inactive_returns_search_false(app_client):
    """POST /ping with is_active=false (default) -> search=false, interval=60."""
    reg = await _register(app_client, email="inactive@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="inactive-dev")
    # is_active defaults to false after registration — no start_search call

    resp = await app_client.post(
        PING_URL,
        json=_ping_body(),
        headers=_device_headers(pairing["device_token"], "inactive-dev"),
    )

    assert resp.status_code == 200
    data = resp.json()
    assert data["search"] is False
    assert data["force_update"] is False
    assert data["interval_seconds"] == 60
    assert data["filters"]["min_price"] == 20.0


# ---------------------------------------------------------------------------
# Test 5: is_active=true, outside schedule → search=false
# ---------------------------------------------------------------------------


async def test_ping_outside_schedule_returns_search_false(app_client, db_session):
    """POST /ping when is_active but outside working hours -> search=false."""
    reg = await _register(app_client, email="outside@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="outside-dev")
    await _verify_email_in_db(db_session, reg["user_id"])
    await _start_search(app_client, reg["access_token"])

    user_id = UUID(reg["user_id"])
    await _update_filters(
        db_session,
        user_id,
        start_time="09:00",
        working_time=8,
        working_days=["MON", "TUE", "WED", "THU", "FRI"],
    )

    # Wednesday 20:00 UTC - AFTER 09:00-17:00 schedule
    now = datetime(2024, 3, 13, 20, 0, tzinfo=ZoneInfo("UTC"))
    with _patch_now(now):
        resp = await app_client.post(
            PING_URL,
            json=_ping_body(timezone="UTC"),
            headers=_device_headers(pairing["device_token"], "outside-dev"),
        )

    assert resp.status_code == 200
    data = resp.json()
    assert data["search"] is False
    assert data["interval_seconds"] == 60


# ---------------------------------------------------------------------------
# Test 6: is_active=true, within schedule → search=true, filters, interval
# ---------------------------------------------------------------------------


async def test_ping_within_schedule_returns_search_true(app_client, db_session):
    """POST /ping when is_active and within working hours -> search=true."""
    reg = await _register(app_client, email="within@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="within-dev")
    await _verify_email_in_db(db_session, reg["user_id"])
    await _start_search(app_client, reg["access_token"])

    user_id = UUID(reg["user_id"])
    await _update_filters(
        db_session,
        user_id,
        start_time="09:00",
        working_time=8,
        working_days=["MON", "TUE", "WED", "THU", "FRI"],
    )

    # Wednesday 14:00 UTC - WITHIN 09:00-17:00 schedule
    now = datetime(2024, 3, 13, 14, 0, tzinfo=ZoneInfo("UTC"))
    with _patch_now(now):
        resp = await app_client.post(
            PING_URL,
            json=_ping_body(timezone="UTC"),
            headers=_device_headers(pairing["device_token"], "within-dev"),
        )

    assert resp.status_code == 200
    data = resp.json()
    assert data["search"] is True
    assert data["interval_seconds"] == 30
    assert data["force_update"] is False
    assert data["filters"]["min_price"] == 20.0


# ---------------------------------------------------------------------------
# Test 7: overnight schedule (22:00 start, 10h) at 05:00 → search=true
# ---------------------------------------------------------------------------


async def test_ping_overnight_schedule_returns_search_true(app_client, db_session):
    """POST /ping during overnight shift (started yesterday) -> search=true."""
    reg = await _register(app_client, email="overnight@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="overnight-dev")
    await _verify_email_in_db(db_session, reg["user_id"])
    await _start_search(app_client, reg["access_token"])

    user_id = UUID(reg["user_id"])
    await _update_filters(
        db_session,
        user_id,
        start_time="22:00",
        working_time=10,
        working_days=["MON", "TUE", "WED", "THU", "FRI"],
    )

    # Saturday 05:00 UTC - within FRI 22:00 - SAT 08:00
    now = datetime(2024, 3, 16, 5, 0, tzinfo=ZoneInfo("UTC"))
    with _patch_now(now):
        resp = await app_client.post(
            PING_URL,
            json=_ping_body(timezone="UTC"),
            headers=_device_headers(pairing["device_token"], "overnight-dev"),
        )

    assert resp.status_code == 200
    assert resp.json()["search"] is True


# ---------------------------------------------------------------------------
# Test 8: stats batch dedup — first ping saves, second with same batch_id skips
# ---------------------------------------------------------------------------


async def test_ping_batch_dedup_first_saves_second_skips(app_client, db_session):
    """Two pings with same batch_id -> only first saves accept_failures."""
    reg = await _register(app_client, email="dedup@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="dedup-dev")
    await _verify_email_in_db(db_session, reg["user_id"])
    await _start_search(app_client, reg["access_token"])
    # Default filters: working_time=24, all days → always within schedule

    stats = {
        "batch_id": "dedup-test-batch-001",
        "cycles_since_last_ping": 1,
        "rides_found": 0,
        "accept_failures": [
            {
                "reason": "AcceptButtonNotFound",
                "ride_price": 25.50,
                "pickup_time": "Tomorrow \u00b7 6:05AM",
                "timestamp": "2024-03-13T10:30:00Z",
            }
        ],
    }

    # First ping — failures saved
    resp1 = await app_client.post(
        PING_URL,
        json=_ping_body(stats=stats),
        headers=_device_headers(pairing["device_token"], "dedup-dev"),
    )
    assert resp1.status_code == 200

    # Second ping — same batch_id, should be skipped
    resp2 = await app_client.post(
        PING_URL,
        json=_ping_body(stats=stats),
        headers=_device_headers(pairing["device_token"], "dedup-dev"),
    )
    assert resp2.status_code == 200

    # Only 1 failure saved (not 2)
    user_id = UUID(reg["user_id"])
    result = await db_session.execute(
        select(func.count()).select_from(AcceptFailure).where(AcceptFailure.user_id == user_id)
    )
    count = result.scalar()
    assert count == 1


# ---------------------------------------------------------------------------
# Test 9: device state update — last_ping_at, timezone updated
# ---------------------------------------------------------------------------


async def test_ping_updates_device_state(app_client, db_session):
    """POST /ping updates last_ping_at, timezone, and device_health in DB."""
    reg = await _register(app_client, email="state@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="state-dev")

    resp = await app_client.post(
        PING_URL,
        json=_ping_body(
            timezone="Europe/Kyiv",
            device_health={
                "accessibility_enabled": True,
                "lyft_running": True,
                "screen_on": False,
            },
        ),
        headers=_device_headers(pairing["device_token"], "state-dev"),
    )
    assert resp.status_code == 200

    user_id = UUID(reg["user_id"])
    result = await db_session.execute(select(PairedDevice).where(PairedDevice.user_id == user_id))
    device = result.scalar_one()

    assert device.last_ping_at is not None
    assert device.timezone == "Europe/Kyiv"
    assert device.accessibility_enabled is True
    assert device.lyft_running is True
    assert device.screen_on is False


# ---------------------------------------------------------------------------
# Test 10: full happy path — valid request → complete response
# ---------------------------------------------------------------------------


async def test_ping_happy_path_complete_response(app_client, db_session):
    """POST /ping with all fields -> correct response + device state updated."""
    reg = await _register(app_client, email="happy@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="happy-dev")
    await _verify_email_in_db(db_session, reg["user_id"])
    await _start_search(app_client, reg["access_token"])
    # Default filters: working_time=24, all days, min_price=20.0

    resp = await app_client.post(
        PING_URL,
        json=_ping_body(
            timezone="America/New_York",
            app_version="2.0.0",
            device_health={
                "accessibility_enabled": True,
                "lyft_running": True,
                "screen_on": True,
            },
            stats={
                "batch_id": "happy-batch-001",
                "cycles_since_last_ping": 5,
                "rides_found": 2,
                "accept_failures": [],
            },
        ),
        headers=_device_headers(pairing["device_token"], "happy-dev"),
    )

    assert resp.status_code == 200
    data = resp.json()
    assert data["search"] is True
    assert data["interval_seconds"] == 30
    assert data["force_update"] is False
    assert data["update_url"] is None
    assert data["filters"]["min_price"] == 20.0

    # Verify device state
    user_id = UUID(reg["user_id"])
    result = await db_session.execute(select(PairedDevice).where(PairedDevice.user_id == user_id))
    device = result.scalar_one()
    assert device.last_ping_at is not None
    assert device.last_interval_sent == 30
    assert device.accessibility_enabled is True


# ---------------------------------------------------------------------------
# Test 11: E2E integration flow with real DB and Redis
# ---------------------------------------------------------------------------


async def test_ping_e2e_flow(app_client, db_session):
    """Full E2E: register → pair → ping (inactive) → start search → ping (active)."""
    # 1. Register
    reg = await _register(app_client, email="e2e@example.com")

    # 2. Pair device
    pairing = await _pair_device(app_client, reg["access_token"], device_id="e2e-dev")

    # 3. First ping — search not started → search=false
    resp1 = await app_client.post(
        PING_URL,
        json=_ping_body(),
        headers=_device_headers(pairing["device_token"], "e2e-dev"),
    )
    assert resp1.json()["search"] is False
    assert resp1.json()["interval_seconds"] == 60

    # 4. Start search
    await _verify_email_in_db(db_session, reg["user_id"])
    await _start_search(app_client, reg["access_token"])

    # 5. Second ping — search active, 24h schedule → search=true
    resp2 = await app_client.post(
        PING_URL,
        json=_ping_body(),
        headers=_device_headers(pairing["device_token"], "e2e-dev"),
    )
    assert resp2.json()["search"] is True
    assert resp2.json()["interval_seconds"] == 30

    # 6. Verify device state in DB
    user_id = UUID(reg["user_id"])
    result = await db_session.execute(select(PairedDevice).where(PairedDevice.user_id == user_id))
    device = result.scalar_one()
    assert device.last_ping_at is not None
    assert device.last_interval_sent == 30


# ---------------------------------------------------------------------------
# Test 12: concurrent pings don't create duplicate failures
# ---------------------------------------------------------------------------


async def test_ping_concurrent_no_duplicate_failures(app_client, db_session):
    """Three pings with same batch_id -> only 2 failures saved (from first ping)."""
    reg = await _register(app_client, email="concurrent@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="concurrent-dev")
    await _verify_email_in_db(db_session, reg["user_id"])
    await _start_search(app_client, reg["access_token"])

    stats = {
        "batch_id": "concurrent-batch-001",
        "cycles_since_last_ping": 1,
        "rides_found": 0,
        "accept_failures": [
            {
                "reason": "AcceptButtonNotFound",
                "ride_price": 25.50,
                "pickup_time": "Tomorrow \u00b7 6:05AM",
                "timestamp": "2024-03-13T10:30:00Z",
            },
            {
                "reason": "TimeoutExpired",
                "ride_price": 30.00,
                "pickup_time": "Today \u00b7 3:00PM",
                "timestamp": "2024-03-13T10:31:00Z",
            },
        ],
    }

    # Simulate 3 pings with the same batch_id (retries)
    for _ in range(3):
        resp = await app_client.post(
            PING_URL,
            json=_ping_body(stats=stats),
            headers=_device_headers(pairing["device_token"], "concurrent-dev"),
        )
        assert resp.status_code == 200

    # Only 2 failures saved (from the first ping), not 6
    user_id = UUID(reg["user_id"])
    result = await db_session.execute(
        select(func.count()).select_from(AcceptFailure).where(AcceptFailure.user_id == user_id)
    )
    count = result.scalar()
    assert count == 2


# ---------------------------------------------------------------------------
# Test 13: working_days filtering — within hours but non-working day → search=false
# ---------------------------------------------------------------------------


async def test_ping_non_working_day_returns_search_false(app_client, db_session):
    """POST /ping on a non-working day (within hours) -> search=false.

    Schedule: MON-FRI 09:00-17:00.  Ping arrives Saturday 12:00 UTC.
    Even though 12:00 is within the 09:00-17:00 time window, Saturday
    is not in working_days, so search must be false.
    """
    reg = await _register(app_client, email="workdays@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="workdays-dev")
    await _verify_email_in_db(db_session, reg["user_id"])
    await _start_search(app_client, reg["access_token"])

    user_id = UUID(reg["user_id"])
    await _update_filters(
        db_session,
        user_id,
        start_time="09:00",
        working_time=8,
        working_days=["MON", "TUE", "WED", "THU", "FRI"],
    )

    # Saturday 2024-03-16 12:00 UTC — within 09:00-17:00 time window but SAT ∉ working_days
    now = datetime(2024, 3, 16, 12, 0, tzinfo=ZoneInfo("UTC"))
    with _patch_now(now):
        resp = await app_client.post(
            PING_URL,
            json=_ping_body(timezone="UTC"),
            headers=_device_headers(pairing["device_token"], "workdays-dev"),
        )

    assert resp.status_code == 200
    data = resp.json()
    assert data["search"] is False
    assert data["interval_seconds"] == 60


# ---------------------------------------------------------------------------
# Test 14: dynamic interval — active search with interval config returns dynamic value
# ---------------------------------------------------------------------------

_WEIGHTS = [
    5.23,
    5.19,
    4.97,
    4.28,
    3.07,
    1.0,
    1.0,
    1.0,
    1.0,
    1.0,
    3.69,
    5.10,
    6.24,
    4.96,
    5.06,
    5.18,
    4.59,
    4.57,
    5.91,
    5.58,
    5.98,
    5.29,
    5.15,
    4.96,
]


async def _seed_interval_configs(db_session):
    """Seed interval configs in DB for dynamic interval tests."""
    db_session.add(AppConfig(key="requests_per_day", value="1920"))
    db_session.add(AppConfig(key="requests_per_hour", value=json.dumps(_WEIGHTS)))
    await db_session.commit()


async def test_ping_dynamic_interval_peak_hour(app_client, db_session):
    """POST /ping at peak hour with interval config -> dynamic interval_seconds."""
    reg = await _register(app_client, email="dynamic@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="dynamic-dev")
    await _verify_email_in_db(db_session, reg["user_id"])
    await _start_search(app_client, reg["access_token"])
    await _seed_interval_configs(db_session)

    # Wednesday 12:30 UTC -> hour 12, weight 6.24
    # 6.24% of 1920 = ~120 rph -> 30s total -> 30 - 15 = 15s interval
    now = datetime(2024, 3, 13, 12, 30, tzinfo=ZoneInfo("UTC"))
    with _patch_now(now):
        resp = await app_client.post(
            PING_URL,
            json=_ping_body(timezone="UTC", last_cycle_duration_ms=15000),
            headers=_device_headers(pairing["device_token"], "dynamic-dev"),
        )

    assert resp.status_code == 200
    data = resp.json()
    assert data["search"] is True
    assert data["interval_seconds"] == 15


# ---------------------------------------------------------------------------
# Test 15: dynamic interval — off-peak hour returns longer interval
# ---------------------------------------------------------------------------


async def test_ping_dynamic_interval_off_peak_hour(app_client, db_session):
    """POST /ping at off-peak hour with interval config -> longer interval."""
    reg = await _register(app_client, email="offpeak@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="offpeak-dev")
    await _verify_email_in_db(db_session, reg["user_id"])
    await _start_search(app_client, reg["access_token"])
    await _seed_interval_configs(db_session)

    # Wednesday 05:30 UTC -> hour 5, weight 1.0
    # 1.0% of 1920 = 19.2 rph -> 187.5s total -> 187.5 - 15 = 172.5 -> 172s
    now = datetime(2024, 3, 13, 5, 30, tzinfo=ZoneInfo("UTC"))
    with _patch_now(now):
        resp = await app_client.post(
            PING_URL,
            json=_ping_body(timezone="UTC", last_cycle_duration_ms=15000),
            headers=_device_headers(pairing["device_token"], "offpeak-dev"),
        )

    assert resp.status_code == 200
    data = resp.json()
    assert data["search"] is True
    assert data["interval_seconds"] == 172


# ---------------------------------------------------------------------------
# Test 16: no interval config in DB -> fallback to DEFAULT_SEARCH_INTERVAL_SECONDS
# ---------------------------------------------------------------------------


async def test_ping_no_interval_config_falls_back_to_default(app_client, db_session):
    """POST /ping without interval config -> default 30s interval."""
    reg = await _register(app_client, email="fallback@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="fallback-dev")
    await _verify_email_in_db(db_session, reg["user_id"])
    await _start_search(app_client, reg["access_token"])
    # No _seed_interval_configs call -> fallback behavior

    resp = await app_client.post(
        PING_URL,
        json=_ping_body(),
        headers=_device_headers(pairing["device_token"], "fallback-dev"),
    )

    assert resp.status_code == 200
    data = resp.json()
    assert data["search"] is True
    assert data["interval_seconds"] == 30
