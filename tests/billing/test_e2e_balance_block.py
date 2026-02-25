"""E2E tests for Balance block flow.

Verifies that search is blocked when user has zero credits:
- POST /search/start returns 403 INSUFFICIENT_CREDITS
- POST /ping returns search: false with reason: "NO_CREDITS"
- Credits depletion through rides leads to search being blocked

Test strategy (task 14.7):
1. Test 403 INSUFFICIENT_CREDITS when starting search with zero balance
2. Test ping response with reason='NO_CREDITS'
3. Test credits depletion through rides -> search blocked
"""

import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.credit_balance import CreditBalance
from app.models.user import User
from tests.billing.helpers import assert_balance, make_ride_hash


async def _verify_email(db_session: AsyncSession, user_id: str) -> None:
    """Set email_verified=True for a test user (bypass email verification flow)."""
    result = await db_session.execute(select(User).where(User.id == user_id))
    user = result.scalar_one()
    user.email_verified = True
    await db_session.flush()


SEARCH_START_URL = "/api/v1/search/start"
SEARCH_STATUS_URL = "/api/v1/search/status"
RIDES_URL = "/api/v1/rides"
PING_URL = "/api/v1/ping"


def _make_ride_request(
    *, price: float, ride_hash: str | None = None, timezone: str = "America/New_York"
) -> dict:
    """Build a valid POST /rides request body."""
    return {
        "idempotency_key": str(uuid.uuid4()),
        "event_type": "ACCEPTED",
        "ride_hash": ride_hash or make_ride_hash(),
        "timezone": timezone,
        "ride_data": {
            "price": price,
            "pickup_time": "Tomorrow \u00b7 6:05AM",
            "pickup_location": "123 Main St",
            "dropoff_location": "456 Oak Ave",
        },
    }


def _make_ping_request(*, timezone: str = "America/New_York") -> dict:
    """Build a valid POST /ping request body."""
    return {
        "timezone": timezone,
        "app_version": "1.0.0",
    }


# ---------------------------------------------------------------------------
# Test: POST /search/start -> 403 INSUFFICIENT_CREDITS when balance == 0
# ---------------------------------------------------------------------------


async def test_search_blocked_when_zero_credits(
    device_headers, billing_app_config, db_session, fake_redis
):
    """POST /search/start -> 403 INSUFFICIENT_CREDITS when balance is 0.

    The user has been registered via device_headers (which depends on
    authenticated_client), and gets 10 credits via registration bonus.
    We drain balance to 0, then attempt to start search.
    """
    # Drain balance to 0 in DB and Redis cache
    db_session.expire_all()
    result = await db_session.execute(
        select(CreditBalance).where(CreditBalance.user_id == device_headers.user_id)
    )
    balance_row = result.scalar_one()
    balance_row.balance = 0
    await db_session.flush()
    await fake_redis.setex(f"user_balance:{device_headers.user_id}", 300, "0")

    # POST /search/start should return 403
    resp = await device_headers.client.post(
        SEARCH_START_URL,
        headers=device_headers.auth_headers,
    )
    assert resp.status_code == 403

    error = resp.json()
    assert error["error"]["code"] == "INSUFFICIENT_CREDITS"


# ---------------------------------------------------------------------------
# Test: POST /ping -> search: false, reason: "NO_CREDITS" when balance == 0
# ---------------------------------------------------------------------------


async def test_ping_returns_no_credits_reason(
    device_headers, billing_app_config, db_session, fake_redis
):
    """POST /ping -> search: false, reason: 'NO_CREDITS' when balance is 0.

    Ping uses device auth (X-Device-Token + X-Device-Id). When balance <= 0,
    the ping handler returns search=false with reason='NO_CREDITS'.
    The balance check in ping handler is independent of is_active state.
    """
    # Drain balance to 0 in DB and Redis cache
    db_session.expire_all()
    result = await db_session.execute(
        select(CreditBalance).where(CreditBalance.user_id == device_headers.user_id)
    )
    balance_row = result.scalar_one()
    balance_row.balance = 0
    await db_session.flush()
    await fake_redis.setex(f"user_balance:{device_headers.user_id}", 300, "0")

    # POST /ping with device auth should return NO_CREDITS
    resp = await device_headers.client.post(
        PING_URL,
        json=_make_ping_request(),
        headers=device_headers.headers,
    )
    assert resp.status_code == 200

    data = resp.json()
    assert data["search"] is False
    assert data["reason"] == "NO_CREDITS"


# ---------------------------------------------------------------------------
# Test: Credits depleted after rides -> search blocked
# ---------------------------------------------------------------------------


async def test_credits_depleted_after_rides(
    device_headers, billing_app_config, db_session, fake_redis
):
    """Deplete all credits through rides -> POST /search/start returns 403.

    Start with 10 credits (registration bonus). Create rides that cost
    2 credits each ($25 price -> Tier 2). 5 rides = 10 credits = 0 balance.
    Then verify search is blocked.
    """
    # Verify email so search/start works (email check comes after balance check)
    await _verify_email(db_session, device_headers.user_id)

    # Create 5 rides at $25 each (2 credits per ride = 10 credits total)
    for _i in range(5):
        resp = await device_headers.client.post(
            RIDES_URL,
            json=_make_ride_request(price=25.00),
            headers=device_headers.headers,
        )
        assert resp.status_code == 201

    # Balance should be 0
    await assert_balance(db_session, device_headers.user_id, 0)

    # POST /search/start should now return 403
    resp = await device_headers.client.post(
        SEARCH_START_URL,
        headers=device_headers.auth_headers,
    )
    assert resp.status_code == 403
    assert resp.json()["error"]["code"] == "INSUFFICIENT_CREDITS"

    # Ping should return NO_CREDITS
    resp = await device_headers.client.post(
        PING_URL,
        json=_make_ping_request(),
        headers=device_headers.headers,
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["search"] is False
    assert data["reason"] == "NO_CREDITS"


# ---------------------------------------------------------------------------
# Test: GET /search/status shows zero balance after depletion
# ---------------------------------------------------------------------------


async def test_search_status_shows_zero_balance(
    device_headers, billing_app_config, db_session, fake_redis
):
    """GET /search/status -> credits_balance=0 after depleting all credits.

    Verifies that the balance exposed via search/status endpoint
    reflects the actual depleted state.
    """
    # Drain balance to 0 in DB and Redis cache
    db_session.expire_all()
    result = await db_session.execute(
        select(CreditBalance).where(CreditBalance.user_id == device_headers.user_id)
    )
    balance_row = result.scalar_one()
    balance_row.balance = 0
    await db_session.flush()
    await fake_redis.setex(f"user_balance:{device_headers.user_id}", 300, "0")

    resp = await device_headers.client.get(
        SEARCH_STATUS_URL,
        headers=device_headers.auth_headers,
    )
    assert resp.status_code == 200

    data = resp.json()
    assert data["credits_balance"] == 0
