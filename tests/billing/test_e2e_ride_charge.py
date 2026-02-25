"""E2E tests for Ride charge flow with tier matching.

Verifies the complete ride charge flow: ride acceptance via POST /rides,
credit charging based on price tiers, CreditTransaction creation,
verification status initialization, and edge cases (zero/partial balance).

Test strategy (task 14.4):
1. Parametrized test for all credit tiers (boundaries included)
2. Test edge cases: zero balance, partial balance
3. Verify CreditTransaction created with correct reference_id
4. Verify verification_deadline calculated correctly
"""

import uuid

import pytest
from sqlalchemy import select

from app.models.credit_balance import CreditBalance
from app.models.credit_transaction import CreditTransaction, TransactionType
from app.models.ride import Ride
from tests.billing.helpers import assert_balance, assert_transaction_exists, make_ride_hash

RIDES_URL = "/api/v1/rides"


def _make_ride_request(
    *, price: float, ride_hash: str | None = None, timezone: str = "America/New_York"
) -> dict:
    """Build a valid POST /rides request body with unique idempotency_key."""
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


# ---------------------------------------------------------------------------
# Tier matching (parametrized)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "ride_price,expected_credits",
    [
        (15.00, 1),  # Tier 1: <=20 -> 1 credit
        (20.00, 1),  # Tier 1 boundary: $20 exact -> 1 credit
        (45.00, 2),  # Tier 2: <=50 -> 2 credits
        (50.00, 2),  # Tier 2 boundary: $50 exact -> 2 credits
        (100.00, 3),  # Tier 3: >50 -> 3 credits (catch-all)
    ],
)
async def test_ride_charge_tier_matching(
    device_headers, billing_app_config, db_session, ride_price, expected_credits
):
    """POST /rides -> credits charged match the price tier."""
    initial_balance = 10  # registration bonus

    resp = await device_headers.client.post(
        RIDES_URL,
        json=_make_ride_request(price=ride_price),
        headers=device_headers.headers,
    )
    assert resp.status_code == 201

    ride_id = resp.json()["ride_id"]

    # Verify ride has correct credits_charged
    result = await db_session.execute(select(Ride).where(Ride.id == ride_id))
    ride = result.scalar_one()
    assert ride.credits_charged == expected_credits

    # Verify balance decreased correctly
    await assert_balance(db_session, device_headers.user_id, initial_balance - expected_credits)


# ---------------------------------------------------------------------------
# CreditTransaction RIDE_CHARGE
# ---------------------------------------------------------------------------


async def test_ride_creates_charge_transaction(device_headers, billing_app_config, db_session):
    """POST /rides -> CreditTransaction(RIDE_CHARGE) created with correct fields."""
    resp = await device_headers.client.post(
        RIDES_URL,
        json=_make_ride_request(price=45.00),  # Tier 2 -> 2 credits
        headers=device_headers.headers,
    )
    assert resp.status_code == 201
    ride_id = resp.json()["ride_id"]

    # RIDE_CHARGE transaction with negative amount
    tx = await assert_transaction_exists(
        db_session, device_headers.user_id, TransactionType.RIDE_CHARGE, -2
    )
    assert tx.balance_after == 8  # 10 (bonus) - 2
    assert str(tx.reference_id) == ride_id


# ---------------------------------------------------------------------------
# Verification status and deadline
# ---------------------------------------------------------------------------


async def test_ride_verification_pending_with_deadline(
    device_headers, billing_app_config, db_session
):
    """POST /rides -> verification_status=PENDING and verification_deadline set."""
    resp = await device_headers.client.post(
        RIDES_URL,
        json=_make_ride_request(price=25.00),
        headers=device_headers.headers,
    )
    assert resp.status_code == 201

    result = await db_session.execute(select(Ride).where(Ride.id == resp.json()["ride_id"]))
    ride = result.scalar_one()
    assert ride.verification_status == "PENDING"
    assert ride.verification_deadline is not None
    assert ride.ride_hash is not None
    assert len(ride.ride_hash) == 64


# ---------------------------------------------------------------------------
# Zero balance edge case
# ---------------------------------------------------------------------------


async def test_ride_zero_balance_no_charge(device_headers, billing_app_config, db_session):
    """balance=0 -> ride created without charge, credits_charged=0, no RIDE_CHARGE tx."""
    # Drain balance to 0 before the ride
    result = await db_session.execute(
        select(CreditBalance).where(CreditBalance.user_id == device_headers.user_id)
    )
    balance_row = result.scalar_one()
    balance_row.balance = 0
    await db_session.flush()

    resp = await device_headers.client.post(
        RIDES_URL,
        json=_make_ride_request(price=45.00),
        headers=device_headers.headers,
    )
    assert resp.status_code == 201

    # Ride created with credits_charged=0
    ride_result = await db_session.execute(select(Ride).where(Ride.id == resp.json()["ride_id"]))
    ride = ride_result.scalar_one()
    assert ride.credits_charged == 0

    # Balance remains 0
    await assert_balance(db_session, device_headers.user_id, 0)

    # No RIDE_CHARGE transaction created
    tx_result = await db_session.execute(
        select(CreditTransaction).where(
            CreditTransaction.user_id == device_headers.user_id,
            CreditTransaction.type == TransactionType.RIDE_CHARGE,
        )
    )
    assert tx_result.scalar_one_or_none() is None


# ---------------------------------------------------------------------------
# Partial balance edge case
# ---------------------------------------------------------------------------


async def test_ride_partial_balance_charge(device_headers, billing_app_config, db_session):
    """balance=1, tier requires 2 -> charge min(2,1)=1 (partial), balance becomes 0."""
    # Set balance to 1 before the ride
    result = await db_session.execute(
        select(CreditBalance).where(CreditBalance.user_id == device_headers.user_id)
    )
    balance_row = result.scalar_one()
    balance_row.balance = 1
    await db_session.flush()

    resp = await device_headers.client.post(
        RIDES_URL,
        json=_make_ride_request(price=45.00),  # Tier 2 -> 2 credits
        headers=device_headers.headers,
    )
    assert resp.status_code == 201

    # Ride charged only 1 credit (partial)
    ride_result = await db_session.execute(select(Ride).where(Ride.id == resp.json()["ride_id"]))
    ride = ride_result.scalar_one()
    assert ride.credits_charged == 1

    # Balance is now 0
    await assert_balance(db_session, device_headers.user_id, 0)

    # RIDE_CHARGE transaction with -1 (not -2)
    tx = await assert_transaction_exists(
        db_session, device_headers.user_id, TransactionType.RIDE_CHARGE, -1
    )
    assert tx.balance_after == 0
