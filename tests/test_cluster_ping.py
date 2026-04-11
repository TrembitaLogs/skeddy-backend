"""Tests for cluster gate integration in ping handler (step 11).

Covers:
- Clustering disabled -> normal behaviour
- Solo device (not in cluster) -> normal behaviour
- Penalized device -> search=false, interval=60
- Active device + search turn -> search=true, cluster interval
- Active device + wait -> search=false, remaining seconds
- Redis error -> fallback to solo
- search_active=false -> step 11 skipped
- Integration with existing ping flow (mock cluster_gate)
"""

from datetime import datetime
from unittest.mock import AsyncMock, patch
from uuid import UUID
from zoneinfo import ZoneInfo

from sqlalchemy import select

from app.models.credit_balance import CreditBalance
from app.models.credit_transaction import CreditTransaction, TransactionType
from app.models.paired_device import PairedDevice
from app.models.search_filters import SearchFilters
from app.models.user import User

PING_URL = "/api/v1/ping"
REGISTER_URL = "/api/v1/auth/register"
SEARCH_LOGIN_URL = "/api/v1/auth/search-login"
SEARCH_START_URL = "/api/v1/search/start"

_TEST_PASSWORD = "securePass1"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _patch_now(target_now: datetime):
    """Patch datetime.now() in ping_service and ping router."""
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
            patch("app.services.ping_service.schedule.datetime", _FakeDatetime),
            patch("app.services.ping_service.device.datetime", _FakeDatetime),
            patch("app.services.ping_service.verification.datetime", _FakeDatetime),
            patch("app.services.ping_service.orchestration.datetime", _FakeDatetime),
        ):
            yield

    return _combined()


async def _register(app_client, email="cluster@example.com"):
    resp = await app_client.post(REGISTER_URL, json={"email": email, "password": _TEST_PASSWORD})
    assert resp.status_code == 201
    return resp.json()


async def _pair_device(app_client, email, device_id="cluster-dev-001"):
    resp = await app_client.post(
        SEARCH_LOGIN_URL,
        json={
            "email": email,
            "password": _TEST_PASSWORD,
            "device_id": device_id,
            "timezone": "UTC",
        },
    )
    assert resp.status_code == 200
    return resp.json()


def _device_headers(device_token: str, device_id: str) -> dict:
    return {"X-Device-Token": device_token, "X-Device-Id": device_id}


def _ping_body(**overrides) -> dict:
    body = {"timezone": "UTC", "app_version": "1.0.0"}
    body.update(overrides)
    return body


def _jwt(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


async def _verify_email_in_db(db_session, user_id: str):
    """Set email_verified=True and grant registration bonus (mirrors verify-email endpoint)."""
    uid = UUID(user_id)
    result = await db_session.execute(select(User).where(User.id == uid))
    user = result.scalar_one()
    user.email_verified = True
    cb_result = await db_session.execute(select(CreditBalance).where(CreditBalance.user_id == uid))
    cb = cb_result.scalar_one()
    cb.balance = 10
    db_session.add(
        CreditTransaction(
            user_id=uid,
            type=TransactionType.REGISTRATION_BONUS,
            amount=10,
            balance_after=10,
        )
    )
    await db_session.commit()


async def _start_search(app_client, access_token):
    resp = await app_client.post(SEARCH_START_URL, headers=_jwt(access_token))
    assert resp.status_code == 200


async def _update_filters(db_session, user_id, **kwargs):
    result = await db_session.execute(
        select(SearchFilters).where(SearchFilters.user_id == user_id)
    )
    filters = result.scalar_one()
    for key, value in kwargs.items():
        setattr(filters, key, value)
    await db_session.commit()


async def _setup_active_device(app_client, db_session, email, device_id):
    """Register, pair, verify email, start search, set schedule -> active device."""
    reg = await _register(app_client, email=email)
    pairing = await _pair_device(app_client, email, device_id=device_id)
    await _verify_email_in_db(db_session, reg["user_id"])
    await _start_search(app_client, reg["access_token"])

    user_id = UUID(reg["user_id"])
    await _update_filters(
        db_session,
        user_id,
        start_time="00:00",
        working_time=24,
        working_days=["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"],
    )
    return reg, pairing


# ---------------------------------------------------------------------------
# Test 1: Clustering disabled -> cluster_gate returns None -> normal behaviour
# ---------------------------------------------------------------------------


async def test_cluster_disabled_normal_behaviour(app_client, db_session):
    """When clustering_enabled=false, cluster_gate returns None and ping
    returns the normal solo interval."""
    _reg, pairing = await _setup_active_device(
        app_client, db_session, "cdis@example.com", "cdis-dev"
    )

    now = datetime(2024, 3, 13, 14, 0, tzinfo=ZoneInfo("UTC"))
    with (
        _patch_now(now),
        patch(
            "app.services.ping_service.orchestration.cluster_gate",
            new_callable=AsyncMock,
            return_value=None,
        ) as mock_cg,
    ):
        resp = await app_client.post(
            PING_URL,
            json=_ping_body(),
            headers=_device_headers(pairing["device_token"], "cdis-dev"),
        )

    assert resp.status_code == 200
    data = resp.json()
    assert data["search"] is True
    # cluster_gate was called with clustering_enabled from config (default False)
    mock_cg.assert_called_once()
    # Normal interval (default 30s)
    assert data["interval_seconds"] == 30


# ---------------------------------------------------------------------------
# Test 2: Solo device (not in cluster) -> cluster_gate returns None
# ---------------------------------------------------------------------------


async def test_solo_device_normal_behaviour(app_client, db_session):
    """Solo device (no cluster key in Redis) -> cluster_gate returns None,
    ping returns normal solo interval."""
    _reg, pairing = await _setup_active_device(
        app_client, db_session, "solo@example.com", "solo-dev"
    )

    now = datetime(2024, 3, 13, 14, 0, tzinfo=ZoneInfo("UTC"))
    with (
        _patch_now(now),
        patch(
            "app.services.ping_service.orchestration.cluster_gate",
            new_callable=AsyncMock,
            return_value=None,
        ),
    ):
        resp = await app_client.post(
            PING_URL,
            json=_ping_body(),
            headers=_device_headers(pairing["device_token"], "solo-dev"),
        )

    assert resp.status_code == 200
    data = resp.json()
    assert data["search"] is True
    assert data["interval_seconds"] == 30


# ---------------------------------------------------------------------------
# Test 3: Penalized device -> search=false, interval=60
# ---------------------------------------------------------------------------


async def test_penalized_device_search_false(app_client, db_session):
    """Penalized device in cluster -> search=false, interval=60."""
    _reg, pairing = await _setup_active_device(
        app_client, db_session, "pen@example.com", "pen-dev"
    )

    now = datetime(2024, 3, 13, 14, 0, tzinfo=ZoneInfo("UTC"))
    with (
        _patch_now(now),
        patch(
            "app.services.ping_service.orchestration.cluster_gate",
            new_callable=AsyncMock,
            return_value={"search": False, "interval_seconds": 60},
        ),
    ):
        resp = await app_client.post(
            PING_URL,
            json=_ping_body(),
            headers=_device_headers(pairing["device_token"], "pen-dev"),
        )

    assert resp.status_code == 200
    data = resp.json()
    assert data["search"] is False
    assert data["interval_seconds"] == 60


# ---------------------------------------------------------------------------
# Test 4: Active device + search turn -> search=true, cluster interval
# ---------------------------------------------------------------------------


async def test_cluster_search_turn(app_client, db_session):
    """Active device wins the search slot -> search=true, cluster interval."""
    _reg, pairing = await _setup_active_device(
        app_client, db_session, "turn@example.com", "turn-dev"
    )

    now = datetime(2024, 3, 13, 14, 0, tzinfo=ZoneInfo("UTC"))
    with (
        _patch_now(now),
        patch(
            "app.services.ping_service.orchestration.cluster_gate",
            new_callable=AsyncMock,
            return_value={"search": True, "interval_seconds": 45},
        ),
    ):
        resp = await app_client.post(
            PING_URL,
            json=_ping_body(),
            headers=_device_headers(pairing["device_token"], "turn-dev"),
        )

    assert resp.status_code == 200
    data = resp.json()
    assert data["search"] is True
    assert data["interval_seconds"] == 45


# ---------------------------------------------------------------------------
# Test 5: Active device + wait -> search=false, remaining seconds
# ---------------------------------------------------------------------------


async def test_cluster_wait(app_client, db_session):
    """Active device must wait -> search=false, remaining seconds."""
    _reg, pairing = await _setup_active_device(
        app_client, db_session, "wait@example.com", "wait-dev"
    )

    now = datetime(2024, 3, 13, 14, 0, tzinfo=ZoneInfo("UTC"))
    with (
        _patch_now(now),
        patch(
            "app.services.ping_service.orchestration.cluster_gate",
            new_callable=AsyncMock,
            return_value={"search": False, "interval_seconds": 12},
        ),
    ):
        resp = await app_client.post(
            PING_URL,
            json=_ping_body(),
            headers=_device_headers(pairing["device_token"], "wait-dev"),
        )

    assert resp.status_code == 200
    data = resp.json()
    assert data["search"] is False
    assert data["interval_seconds"] == 12


# ---------------------------------------------------------------------------
# Test 6: Redis error in cluster_gate -> fallback to solo (None)
# ---------------------------------------------------------------------------


async def test_cluster_redis_error_fallback(app_client, db_session):
    """Redis error inside cluster_gate -> returns None, ping uses solo logic."""
    _reg, pairing = await _setup_active_device(
        app_client, db_session, "rerr@example.com", "rerr-dev"
    )

    now = datetime(2024, 3, 13, 14, 0, tzinfo=ZoneInfo("UTC"))
    with (
        _patch_now(now),
        patch(
            "app.services.ping_service.orchestration.cluster_gate",
            new_callable=AsyncMock,
            return_value=None,
        ),
    ):
        resp = await app_client.post(
            PING_URL,
            json=_ping_body(),
            headers=_device_headers(pairing["device_token"], "rerr-dev"),
        )

    assert resp.status_code == 200
    data = resp.json()
    assert data["search"] is True
    assert data["interval_seconds"] == 30


# ---------------------------------------------------------------------------
# Test 7: search_active=false -> step 11 is NOT executed
# ---------------------------------------------------------------------------


async def test_search_inactive_skips_cluster_gate(app_client, db_session):
    """When search_active=false (search not started), cluster_gate is not called."""
    await _register(app_client, email="inact@example.com")
    pairing = await _pair_device(app_client, "inact@example.com", device_id="inact-dev")

    with patch(
        "app.services.ping_service.orchestration.cluster_gate",
        new_callable=AsyncMock,
    ) as mock_cg:
        resp = await app_client.post(
            PING_URL,
            json=_ping_body(),
            headers=_device_headers(pairing["device_token"], "inact-dev"),
        )

    assert resp.status_code == 200
    data = resp.json()
    assert data["search"] is False
    # cluster_gate must NOT be called when search is inactive
    mock_cg.assert_not_called()


# ---------------------------------------------------------------------------
# Test 8: cluster gate overrides interval saved to device
# ---------------------------------------------------------------------------


async def test_cluster_interval_saved_to_device(app_client, db_session):
    """Cluster gate interval is saved to device.last_interval_sent."""
    reg, pairing = await _setup_active_device(
        app_client, db_session, "saved@example.com", "saved-dev"
    )

    user_id = UUID(reg["user_id"])
    now = datetime(2024, 3, 13, 14, 0, tzinfo=ZoneInfo("UTC"))
    with (
        _patch_now(now),
        patch(
            "app.services.ping_service.orchestration.cluster_gate",
            new_callable=AsyncMock,
            return_value={"search": True, "interval_seconds": 90},
        ),
    ):
        resp = await app_client.post(
            PING_URL,
            json=_ping_body(),
            headers=_device_headers(pairing["device_token"], "saved-dev"),
        )

    assert resp.status_code == 200
    assert resp.json()["interval_seconds"] == 90

    result = await db_session.execute(select(PairedDevice).where(PairedDevice.user_id == user_id))
    device = result.scalar_one()
    assert device.last_interval_sent == 90
