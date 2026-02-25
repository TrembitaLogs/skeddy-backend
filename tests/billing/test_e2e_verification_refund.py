"""E2E tests for Verification -> Cancel + Refund flow.

Verifies the complete cancellation flow: ride accepted (PENDING) ->
ping with ride_statuses present=false -> verification_deadline passes ->
next ping triggers auto-cancel with credit refund. Also tests edge cases:
zero-charged ride cancellation and disappeared-then-reappeared ride.

Test strategy (task 14.6):
1. Test full flow: charge -> cancel -> refund (balance restored)
2. Verify no refund when credits_charged=0
3. Test disappeared then reappeared -> confirmed (not cancelled)
"""

import uuid
from datetime import UTC, datetime, timedelta

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.credit_balance import CreditBalance
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
    db_session.expire_all()
    result = await db_session.execute(select(Ride).where(Ride.id == ride_id))
    return result.scalar_one()


async def _set_deadline_to_past(db_session: AsyncSession, ride_id: str) -> None:
    """Set a ride's verification_deadline to 1 minute ago (simulate deadline passing)."""
    ride = await _get_ride(db_session, ride_id)
    ride.verification_deadline = datetime.now(UTC) - timedelta(minutes=1)
    await db_session.flush()


# ---------------------------------------------------------------------------
# Test: Full verification -> cancel + refund flow
# ---------------------------------------------------------------------------


async def test_verification_cancel_refund_after_deadline(
    device_headers, billing_app_config, db_session
):
    """Full flow: ride PENDING -> ping present=false -> deadline passes -> CANCELLED + refund.

    Steps:
    1. Create ride via POST /rides (verification_status=PENDING, credits charged)
    2. Ping with ride_statuses present=false (sets last_reported_present=false)
    3. Set verification_deadline to past (simulate time passing)
    4. Ping again -> process_expired_verifications cancels the ride + refund
    5. Verify ride is CANCELLED with credits_refunded set
    6. Verify balance restored to initial (charge reversed by refund)
    7. Verify CreditTransaction RIDE_REFUND exists with correct reference_id
    """
    initial_balance = 10  # registration bonus
    ride_hash = make_ride_hash()

    # 1. Create ride (PENDING) — $25 maps to 2 credits via ride_credit_tiers
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

    credits_charged = ride.credits_charged
    assert credits_charged > 0  # sanity check: ride was charged

    # Balance should have decreased after charge
    await assert_balance(db_session, device_headers.user_id, initial_balance - credits_charged)

    # 2. Ping with ride_statuses present=false
    resp = await device_headers.client.post(
        PING_URL,
        json=_make_ping_request(ride_statuses=[{"ride_hash": ride_hash, "present": False}]),
        headers=device_headers.headers,
    )
    assert resp.status_code == 200

    ride = await _get_ride(db_session, ride_id)
    assert ride.last_reported_present is False
    assert ride.disappeared_at is not None
    assert ride.verification_status == "PENDING"  # not yet cancelled

    # 3. Set deadline to past
    await _set_deadline_to_past(db_session, ride_id)

    # 4. Ping again -> triggers process_expired_verifications -> CANCELLED + refund
    resp = await device_headers.client.post(
        PING_URL,
        json=_make_ping_request(),
        headers=device_headers.headers,
    )
    assert resp.status_code == 200

    # 5. Verify CANCELLED with refund
    ride = await _get_ride(db_session, ride_id)
    assert ride.verification_status == "CANCELLED"
    assert ride.verified_at is not None
    assert ride.credits_refunded == credits_charged
    ride_id_uuid = ride.id  # capture before expire_all() to avoid lazy-load

    # 6. Balance restored to initial (charge fully reversed)
    await assert_balance(db_session, device_headers.user_id, initial_balance)

    # 7. Verify CreditTransaction RIDE_REFUND
    db_session.expire_all()
    result = await db_session.execute(
        select(CreditTransaction).where(
            CreditTransaction.user_id == device_headers.user_id,
            CreditTransaction.type == TransactionType.RIDE_REFUND,
        )
    )
    refund_tx = result.scalar_one()
    assert refund_tx.amount == credits_charged  # positive amount
    assert refund_tx.reference_id == ride_id_uuid
    assert refund_tx.balance_after == initial_balance


# ---------------------------------------------------------------------------
# Test: No refund when credits_charged == 0
# ---------------------------------------------------------------------------


async def test_no_refund_when_zero_charged(
    device_headers, billing_app_config, db_session, fake_redis
):
    """Ride with credits_charged=0 -> CANCELLED but no refund transaction.

    When a user has 0 balance and a ride is accepted, credits_charged=0.
    On cancellation, no RIDE_REFUND transaction should be created and
    balance should remain 0.
    """
    # Drain balance to 0 (simulate depleted credits)
    db_session.expire_all()
    result = await db_session.execute(
        select(CreditBalance).where(CreditBalance.user_id == device_headers.user_id)
    )
    balance_row = result.scalar_one()
    balance_row.balance = 0
    await db_session.flush()
    await fake_redis.setex(f"user_balance:{device_headers.user_id}", 300, "0")

    ride_hash = make_ride_hash()

    # Create ride (no credits charged because balance=0)
    resp = await device_headers.client.post(
        RIDES_URL,
        json=_make_ride_request(price=25.00, ride_hash=ride_hash),
        headers=device_headers.headers,
    )
    assert resp.status_code == 201
    ride_id = resp.json()["ride_id"]

    ride = await _get_ride(db_session, ride_id)
    assert ride.credits_charged == 0  # no credits to charge

    # Ping with present=false
    resp = await device_headers.client.post(
        PING_URL,
        json=_make_ping_request(ride_statuses=[{"ride_hash": ride_hash, "present": False}]),
        headers=device_headers.headers,
    )
    assert resp.status_code == 200

    # Set deadline to past and trigger verification
    await _set_deadline_to_past(db_session, ride_id)
    resp = await device_headers.client.post(
        PING_URL,
        json=_make_ping_request(),
        headers=device_headers.headers,
    )
    assert resp.status_code == 200

    # Verify CANCELLED but no refund
    ride = await _get_ride(db_session, ride_id)
    assert ride.verification_status == "CANCELLED"
    assert ride.verified_at is not None
    assert ride.credits_refunded == 0

    # Balance should still be 0
    await assert_balance(db_session, device_headers.user_id, 0)

    # No RIDE_REFUND transaction should exist
    db_session.expire_all()
    result = await db_session.execute(
        select(CreditTransaction).where(
            CreditTransaction.user_id == device_headers.user_id,
            CreditTransaction.type == TransactionType.RIDE_REFUND,
        )
    )
    assert result.scalar_one_or_none() is None


# ---------------------------------------------------------------------------
# Test: Ride disappeared then reappeared -> CONFIRMED (not cancelled)
# ---------------------------------------------------------------------------


async def test_ride_disappeared_then_reappeared(device_headers, billing_app_config, db_session):
    """Ride disappears then reappears before deadline -> CONFIRMED (no refund).

    PRD section 6: decision is based on last_reported_present at deadline.
    If the final report is present=true, the ride is CONFIRMED even if
    it temporarily disappeared.
    """
    initial_balance = 10
    ride_hash = make_ride_hash()

    # Create ride
    resp = await device_headers.client.post(
        RIDES_URL,
        json=_make_ride_request(price=25.00, ride_hash=ride_hash),
        headers=device_headers.headers,
    )
    assert resp.status_code == 201
    ride_id = resp.json()["ride_id"]

    ride = await _get_ride(db_session, ride_id)
    credits_charged = ride.credits_charged

    # Ping present=false (ride disappears)
    resp = await device_headers.client.post(
        PING_URL,
        json=_make_ping_request(ride_statuses=[{"ride_hash": ride_hash, "present": False}]),
        headers=device_headers.headers,
    )
    assert resp.status_code == 200

    ride = await _get_ride(db_session, ride_id)
    assert ride.last_reported_present is False
    assert ride.disappeared_at is not None
    disappeared_at = ride.disappeared_at

    # Ping present=true (ride reappears)
    resp = await device_headers.client.post(
        PING_URL,
        json=_make_ping_request(ride_statuses=[{"ride_hash": ride_hash, "present": True}]),
        headers=device_headers.headers,
    )
    assert resp.status_code == 200

    ride = await _get_ride(db_session, ride_id)
    assert ride.last_reported_present is True
    # disappeared_at should NOT be overwritten (first occurrence only, audit data)
    assert ride.disappeared_at == disappeared_at

    # Set deadline to past and trigger verification
    await _set_deadline_to_past(db_session, ride_id)
    resp = await device_headers.client.post(
        PING_URL,
        json=_make_ping_request(),
        headers=device_headers.headers,
    )
    assert resp.status_code == 200

    # Verify CONFIRMED (last status was present=true)
    ride = await _get_ride(db_session, ride_id)
    assert ride.verification_status == "CONFIRMED"
    assert ride.verified_at is not None
    assert ride.credits_refunded == 0  # no refund for confirmed rides

    # Balance = initial - charged (no refund)
    await assert_balance(db_session, device_headers.user_id, initial_balance - credits_charged)

    # No RIDE_REFUND transaction should exist
    db_session.expire_all()
    result = await db_session.execute(
        select(CreditTransaction).where(
            CreditTransaction.user_id == device_headers.user_id,
            CreditTransaction.type == TransactionType.RIDE_REFUND,
        )
    )
    assert result.scalar_one_or_none() is None
