"""E2E tests for Verification -> Confirm flow with deadline wait.

Verifies the complete verification flow: ride accepted (PENDING) ->
ping with ride_statuses present=true -> verification_deadline passes ->
next ping triggers auto-confirm. Also tests verify_rides in ping response
and multiple rides verification simultaneously.

Test strategy (task 14.5):
1. Test sequence: PENDING -> ping present=true -> deadline -> CONFIRMED
2. Verify verify_rides in ping response for PENDING rides
3. Test with multiple rides simultaneously
"""

import uuid
from datetime import UTC, datetime, timedelta

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.credit_transaction import CreditTransaction, TransactionType
from app.models.ride import Ride
from tests.billing.helpers import assert_balance, make_ride_hash

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


def _make_ping_request(
    *, ride_statuses: list[dict] | None = None, timezone: str = "America/New_York"
) -> dict:
    """Build a valid POST /ping request body."""
    body: dict = {
        "timezone": timezone,
        "app_version": "1.0.0",
    }
    if ride_statuses is not None:
        body["ride_statuses"] = ride_statuses
    return body


async def _get_ride(db_session: AsyncSession, ride_id: str) -> Ride:
    """Fetch a ride by ID, expunging stale state first."""
    # Expire all cached state to ensure we read fresh data after endpoint commits
    db_session.expire_all()
    result = await db_session.execute(select(Ride).where(Ride.id == ride_id))
    return result.scalar_one()


async def _set_deadline_to_past(db_session: AsyncSession, ride_id: str) -> None:
    """Set a ride's verification_deadline to 1 minute ago (simulate deadline passing)."""
    ride = await _get_ride(db_session, ride_id)
    ride.verification_deadline = datetime.now(UTC) - timedelta(minutes=1)
    await db_session.flush()


# ---------------------------------------------------------------------------
# Test: Full verification -> confirm flow
# ---------------------------------------------------------------------------


async def test_verification_confirm_after_deadline(device_headers, billing_app_config, db_session):
    """Full flow: ride PENDING -> ping present=true -> deadline passes -> CONFIRMED.

    Steps:
    1. Create ride via POST /rides (verification_status=PENDING)
    2. Ping with ride_statuses present=true (sets last_reported_present=true)
    3. Set verification_deadline to past (simulate time passing)
    4. Ping again -> process_expired_verifications confirms the ride
    5. Verify ride is CONFIRMED with verified_at set
    6. Verify balance unchanged (no refund for confirmed rides)
    """
    initial_balance = 10  # registration bonus
    ride_hash = make_ride_hash()

    # 1. Create ride (PENDING)
    resp = await device_headers.client.post(
        RIDES_URL,
        json=_make_ride_request(price=25.00, ride_hash=ride_hash),
        headers=device_headers.headers,
    )
    assert resp.status_code == 201
    ride_id = resp.json()["ride_id"]

    ride = await _get_ride(db_session, ride_id)
    assert ride.verification_status == "PENDING"
    assert ride.verification_deadline is not None
    assert ride.last_reported_present is None

    credits_charged = ride.credits_charged  # 2 credits for $25

    # 2. Ping with ride_statuses present=true
    resp = await device_headers.client.post(
        PING_URL,
        json=_make_ping_request(ride_statuses=[{"ride_hash": ride_hash, "present": True}]),
        headers=device_headers.headers,
    )
    assert resp.status_code == 200

    ride = await _get_ride(db_session, ride_id)
    assert ride.last_reported_present is True
    assert ride.verification_status == "PENDING"  # not yet confirmed

    # 3. Set deadline to past
    await _set_deadline_to_past(db_session, ride_id)

    # 4. Ping again -> triggers process_expired_verifications
    resp = await device_headers.client.post(
        PING_URL,
        json=_make_ping_request(),
        headers=device_headers.headers,
    )
    assert resp.status_code == 200

    # 5. Verify CONFIRMED
    ride = await _get_ride(db_session, ride_id)
    assert ride.verification_status == "CONFIRMED"
    assert ride.verified_at is not None
    assert ride.credits_refunded == 0  # no refund

    # 6. Balance = initial - charged (no refund)
    await assert_balance(db_session, device_headers.user_id, initial_balance - credits_charged)


# ---------------------------------------------------------------------------
# Test: Confirm with NULL last_reported_present (presumption of ride)
# ---------------------------------------------------------------------------


async def test_verification_confirm_null_reported_present(
    device_headers, billing_app_config, db_session
):
    """Ride with no status reports (offline Search App) -> CONFIRMED by presumption.

    PRD section 6: last_reported_present IS NULL -> CONFIRMED (presumption of ride).
    """
    # 1. Create ride (PENDING)
    resp = await device_headers.client.post(
        RIDES_URL,
        json=_make_ride_request(price=25.00),
        headers=device_headers.headers,
    )
    assert resp.status_code == 201
    ride_id = resp.json()["ride_id"]

    ride = await _get_ride(db_session, ride_id)
    assert ride.last_reported_present is None  # no reports yet

    # 2. Set deadline to past (no ping with ride_statuses — simulates offline)
    await _set_deadline_to_past(db_session, ride_id)

    # 3. Ping triggers verification processing
    resp = await device_headers.client.post(
        PING_URL,
        json=_make_ping_request(),
        headers=device_headers.headers,
    )
    assert resp.status_code == 200

    # 4. Verify CONFIRMED (presumption)
    ride = await _get_ride(db_session, ride_id)
    assert ride.verification_status == "CONFIRMED"
    assert ride.verified_at is not None
    assert ride.credits_refunded == 0


# ---------------------------------------------------------------------------
# Test: verify_rides in ping response
# ---------------------------------------------------------------------------


async def test_ping_returns_verify_rides_for_pending_ride(
    device_headers, billing_app_config, db_session
):
    """Ping response includes verify_rides with ride_hash for PENDING rides.

    build_verify_rides selects PENDING rides with future deadline where
    last_verification_requested_at is NULL (first check).
    """
    ride_hash = make_ride_hash()

    # Create a PENDING ride with future deadline
    resp = await device_headers.client.post(
        RIDES_URL,
        json=_make_ride_request(price=25.00, ride_hash=ride_hash),
        headers=device_headers.headers,
    )
    assert resp.status_code == 201

    # Ping should include the ride in verify_rides
    resp = await device_headers.client.post(
        PING_URL,
        json=_make_ping_request(),
        headers=device_headers.headers,
    )
    assert resp.status_code == 200
    data = resp.json()

    assert "verify_rides" in data
    assert data["verify_rides"] is not None
    ride_hashes = [item["ride_hash"] for item in data["verify_rides"]]
    assert ride_hash in ride_hashes


async def test_ping_verify_rides_empty_after_deadline(
    device_headers, billing_app_config, db_session
):
    """verify_rides does NOT include rides with expired deadline.

    build_verify_rides requires verification_deadline > now.
    After deadline passes, the ride is no longer in verify_rides
    (it's processed by process_expired_verifications instead).
    """
    ride_hash = make_ride_hash()

    # Create ride
    resp = await device_headers.client.post(
        RIDES_URL,
        json=_make_ride_request(price=25.00, ride_hash=ride_hash),
        headers=device_headers.headers,
    )
    assert resp.status_code == 201
    ride_id = resp.json()["ride_id"]

    # Set deadline to past
    await _set_deadline_to_past(db_session, ride_id)

    # Ping — ride should NOT be in verify_rides (deadline passed)
    resp = await device_headers.client.post(
        PING_URL,
        json=_make_ping_request(),
        headers=device_headers.headers,
    )
    assert resp.status_code == 200
    data = resp.json()

    verify_rides = data.get("verify_rides") or []
    ride_hashes = [item["ride_hash"] for item in verify_rides]
    assert ride_hash not in ride_hashes


async def test_ping_verify_rides_excludes_confirmed_ride(
    device_headers, billing_app_config, db_session
):
    """verify_rides does NOT include already CONFIRMED rides."""
    ride_hash = make_ride_hash()

    # Create ride
    resp = await device_headers.client.post(
        RIDES_URL,
        json=_make_ride_request(price=25.00, ride_hash=ride_hash),
        headers=device_headers.headers,
    )
    assert resp.status_code == 201
    ride_id = resp.json()["ride_id"]

    # Confirm the ride: set present=true, move deadline to past, ping
    await device_headers.client.post(
        PING_URL,
        json=_make_ping_request(ride_statuses=[{"ride_hash": ride_hash, "present": True}]),
        headers=device_headers.headers,
    )
    await _set_deadline_to_past(db_session, ride_id)
    await device_headers.client.post(
        PING_URL,
        json=_make_ping_request(),
        headers=device_headers.headers,
    )

    # Verify ride is CONFIRMED
    ride = await _get_ride(db_session, ride_id)
    assert ride.verification_status == "CONFIRMED"

    # Next ping should NOT include the confirmed ride in verify_rides
    resp = await device_headers.client.post(
        PING_URL,
        json=_make_ping_request(),
        headers=device_headers.headers,
    )
    data = resp.json()
    verify_rides = data.get("verify_rides") or []
    ride_hashes = [item["ride_hash"] for item in verify_rides]
    assert ride_hash not in ride_hashes


# ---------------------------------------------------------------------------
# Test: ride_statuses processing updates ride fields
# ---------------------------------------------------------------------------


async def test_ride_status_report_updates_last_reported_present(
    device_headers, billing_app_config, db_session
):
    """Ping with ride_statuses updates last_reported_present on the ride."""
    ride_hash = make_ride_hash()

    resp = await device_headers.client.post(
        RIDES_URL,
        json=_make_ride_request(price=25.00, ride_hash=ride_hash),
        headers=device_headers.headers,
    )
    assert resp.status_code == 201
    ride_id = resp.json()["ride_id"]

    # Report present=true
    await device_headers.client.post(
        PING_URL,
        json=_make_ping_request(ride_statuses=[{"ride_hash": ride_hash, "present": True}]),
        headers=device_headers.headers,
    )

    ride = await _get_ride(db_session, ride_id)
    assert ride.last_reported_present is True
    assert ride.disappeared_at is None  # never reported absent

    # Report present=false
    await device_headers.client.post(
        PING_URL,
        json=_make_ping_request(ride_statuses=[{"ride_hash": ride_hash, "present": False}]),
        headers=device_headers.headers,
    )

    ride = await _get_ride(db_session, ride_id)
    assert ride.last_reported_present is False
    assert ride.disappeared_at is not None  # first absent report

    disappeared_at = ride.disappeared_at

    # Report present=true again (ride reappeared)
    await device_headers.client.post(
        PING_URL,
        json=_make_ping_request(ride_statuses=[{"ride_hash": ride_hash, "present": True}]),
        headers=device_headers.headers,
    )

    ride = await _get_ride(db_session, ride_id)
    assert ride.last_reported_present is True
    # disappeared_at should NOT be overwritten (audit data, first occurrence only)
    assert ride.disappeared_at == disappeared_at


# ---------------------------------------------------------------------------
# Test: Multiple rides verification simultaneously
# ---------------------------------------------------------------------------


async def test_multiple_rides_all_confirmed(device_headers, billing_app_config, db_session):
    """Multiple PENDING rides -> all present=true -> all CONFIRMED after deadline.

    Creates 3 rides, reports all as present, then triggers verification
    for all simultaneously.
    """
    initial_balance = 10
    ride_hashes = [make_ride_hash() for _ in range(3)]
    ride_ids = []
    total_charged = 0

    # Create 3 rides with different prices
    prices = [15.00, 25.00, 45.00]  # 1 + 2 + 2 = 5 credits total
    for price, ride_hash in zip(prices, ride_hashes, strict=False):
        resp = await device_headers.client.post(
            RIDES_URL,
            json=_make_ride_request(price=price, ride_hash=ride_hash),
            headers=device_headers.headers,
        )
        assert resp.status_code == 201
        ride_ids.append(resp.json()["ride_id"])

        ride = await _get_ride(db_session, ride_ids[-1])
        total_charged += ride.credits_charged

    # Report all as present
    ride_statuses = [{"ride_hash": rh, "present": True} for rh in ride_hashes]
    resp = await device_headers.client.post(
        PING_URL,
        json=_make_ping_request(ride_statuses=ride_statuses),
        headers=device_headers.headers,
    )
    assert resp.status_code == 200

    # Verify all have last_reported_present=true
    for ride_id in ride_ids:
        ride = await _get_ride(db_session, ride_id)
        assert ride.last_reported_present is True

    # Set all deadlines to past
    for ride_id in ride_ids:
        await _set_deadline_to_past(db_session, ride_id)

    # Ping triggers verification for all expired rides
    resp = await device_headers.client.post(
        PING_URL,
        json=_make_ping_request(),
        headers=device_headers.headers,
    )
    assert resp.status_code == 200

    # All rides should be CONFIRMED
    for ride_id in ride_ids:
        ride = await _get_ride(db_session, ride_id)
        assert ride.verification_status == "CONFIRMED"
        assert ride.verified_at is not None
        assert ride.credits_refunded == 0

    # Balance = initial - total_charged (no refunds)
    await assert_balance(db_session, device_headers.user_id, initial_balance - total_charged)

    # No RIDE_REFUND transactions should exist
    db_session.expire_all()
    result = await db_session.execute(
        select(CreditTransaction).where(
            CreditTransaction.user_id == device_headers.user_id,
            CreditTransaction.type == TransactionType.RIDE_REFUND,
        )
    )
    assert result.scalar_one_or_none() is None


# ---------------------------------------------------------------------------
# Test: Verification does not double-process
# ---------------------------------------------------------------------------


async def test_confirmed_ride_not_reprocessed_on_next_ping(
    device_headers, billing_app_config, db_session
):
    """Already CONFIRMED ride is not processed again on subsequent pings.

    Verifies the atomic UPDATE...WHERE verification_status='PENDING' guard.
    """
    ride_hash = make_ride_hash()

    # Create and confirm a ride
    resp = await device_headers.client.post(
        RIDES_URL,
        json=_make_ride_request(price=25.00, ride_hash=ride_hash),
        headers=device_headers.headers,
    )
    ride_id = resp.json()["ride_id"]

    await device_headers.client.post(
        PING_URL,
        json=_make_ping_request(ride_statuses=[{"ride_hash": ride_hash, "present": True}]),
        headers=device_headers.headers,
    )
    await _set_deadline_to_past(db_session, ride_id)
    await device_headers.client.post(
        PING_URL,
        json=_make_ping_request(),
        headers=device_headers.headers,
    )

    ride = await _get_ride(db_session, ride_id)
    assert ride.verification_status == "CONFIRMED"
    first_verified_at = ride.verified_at
    balance_after_confirm = (
        await assert_balance(db_session, device_headers.user_id, 8)
    ).balance  # 10 - 2 = 8

    # Ping again — should NOT reprocess the ride
    await device_headers.client.post(
        PING_URL,
        json=_make_ping_request(),
        headers=device_headers.headers,
    )

    ride = await _get_ride(db_session, ride_id)
    assert ride.verification_status == "CONFIRMED"
    assert ride.verified_at == first_verified_at  # unchanged
    await assert_balance(db_session, device_headers.user_id, balance_after_confirm)


# ---------------------------------------------------------------------------
# Test: verify_rides sent even when search is inactive
# ---------------------------------------------------------------------------


async def test_verify_rides_sent_when_search_inactive(
    device_headers, billing_app_config, db_session, fake_redis
):
    """verify_rides is returned even when search=false (PRD section 7).

    When balance is 0, search=false but verify_rides should still include
    PENDING rides for ongoing verification.
    """
    from app.models.credit_balance import CreditBalance

    ride_hash = make_ride_hash()

    # Create ride first (while balance > 0)
    resp = await device_headers.client.post(
        RIDES_URL,
        json=_make_ride_request(price=25.00, ride_hash=ride_hash),
        headers=device_headers.headers,
    )
    assert resp.status_code == 201

    # Drain balance to 0 in both DB and Redis cache
    # (ping handler reads balance from Redis first via get_balance())
    db_session.expire_all()
    result = await db_session.execute(
        select(CreditBalance).where(CreditBalance.user_id == device_headers.user_id)
    )
    balance_row = result.scalar_one()
    balance_row.balance = 0
    await db_session.flush()
    await fake_redis.setex(f"user_balance:{device_headers.user_id}", 300, "0")

    # Ping — search should be false but verify_rides should include the ride
    resp = await device_headers.client.post(
        PING_URL,
        json=_make_ping_request(),
        headers=device_headers.headers,
    )
    assert resp.status_code == 200
    data = resp.json()

    assert data["search"] is False
    assert data.get("reason") == "NO_CREDITS"

    # verify_rides should still include the PENDING ride
    verify_rides = data.get("verify_rides") or []
    ride_hashes = [item["ride_hash"] for item in verify_rides]
    assert ride_hash in ride_hashes
