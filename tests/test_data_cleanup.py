import asyncio
import logging
import uuid
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy.exc import OperationalError

from app.models.accept_failure import AcceptFailure
from app.models.credit_transaction import CreditTransaction
from app.models.purchase_order import PurchaseOrder
from app.models.ride import Ride
from app.models.user import User
from app.tasks.data_cleanup import (
    cleanup_old_data,
    clear_ride_reference_ids,
    delete_old_accept_failures,
    delete_old_rides,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_user(email: str = "datacleanup@example.com") -> User:
    return User(
        id=uuid.uuid4(),
        email=email,
        password_hash="fakehash",
    )


def _make_ride(user: User, *, weeks_ago: int) -> Ride:
    """Create a Ride with created_at set to *weeks_ago* weeks in the past."""
    created_at = datetime.now(UTC) - timedelta(weeks=weeks_ago)
    return Ride(
        id=uuid.uuid4(),
        user_id=user.id,
        idempotency_key=str(uuid.uuid4()),
        event_type="ACCEPTED",
        ride_data={"price": "25.00", "pickup_location": "Test"},
        ride_hash="a" * 64,
        created_at=created_at,
    )


def _make_accept_failure(user: User, *, weeks_ago: int) -> AcceptFailure:
    """Create an AcceptFailure with reported_at set to *weeks_ago* weeks in the past."""
    reported_at = datetime.now(UTC) - timedelta(weeks=weeks_ago)
    return AcceptFailure(
        id=uuid.uuid4(),
        user_id=user.id,
        reason="ACCESSIBILITY_NOT_ENABLED",
        reported_at=reported_at,
    )


# ---------------------------------------------------------------------------
# delete_old_rides — integration tests (real DB)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_deletes_old_rides(db_session):
    """Rides older than 8 weeks should be deleted."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    old_ride = _make_ride(user, weeks_ago=9)
    db_session.add(old_ride)
    await db_session.flush()

    cutoff = datetime.now(UTC) - timedelta(weeks=8)
    deleted = await delete_old_rides(db_session, cutoff)

    assert deleted == 1

    from sqlalchemy import select

    result = await db_session.execute(select(Ride).where(Ride.id == old_ride.id))
    assert result.scalar_one_or_none() is None


@pytest.mark.asyncio
async def test_keeps_recent_rides(db_session):
    """Rides younger than 8 weeks should NOT be deleted."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    recent_ride = _make_ride(user, weeks_ago=4)
    db_session.add(recent_ride)
    await db_session.flush()

    cutoff = datetime.now(UTC) - timedelta(weeks=8)
    deleted = await delete_old_rides(db_session, cutoff)

    assert deleted == 0

    from sqlalchemy import select

    result = await db_session.execute(select(Ride).where(Ride.id == recent_ride.id))
    assert result.scalar_one_or_none() is not None


@pytest.mark.asyncio
async def test_mixed_old_and_recent_rides(db_session):
    """Only old rides are deleted; recent ones remain."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    old1 = _make_ride(user, weeks_ago=10)
    old2 = _make_ride(user, weeks_ago=52)
    recent1 = _make_ride(user, weeks_ago=1)
    recent2 = _make_ride(user, weeks_ago=7)
    db_session.add_all([old1, old2, recent1, recent2])
    await db_session.flush()

    cutoff = datetime.now(UTC) - timedelta(weeks=8)
    deleted = await delete_old_rides(db_session, cutoff)

    assert deleted == 2

    from sqlalchemy import select

    result = await db_session.execute(select(Ride))
    remaining = result.scalars().all()
    remaining_ids = {r.id for r in remaining}
    assert recent1.id in remaining_ids
    assert recent2.id in remaining_ids
    assert old1.id not in remaining_ids
    assert old2.id not in remaining_ids


# ---------------------------------------------------------------------------
# delete_old_accept_failures — integration tests (real DB)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_deletes_old_accept_failures(db_session):
    """Accept failures older than 8 weeks should be deleted."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    old_failure = _make_accept_failure(user, weeks_ago=9)
    db_session.add(old_failure)
    await db_session.flush()

    cutoff = datetime.now(UTC) - timedelta(weeks=8)
    deleted = await delete_old_accept_failures(db_session, cutoff)

    assert deleted == 1

    from sqlalchemy import select

    result = await db_session.execute(
        select(AcceptFailure).where(AcceptFailure.id == old_failure.id)
    )
    assert result.scalar_one_or_none() is None


@pytest.mark.asyncio
async def test_keeps_recent_accept_failures(db_session):
    """Accept failures younger than 8 weeks should NOT be deleted."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    recent_failure = _make_accept_failure(user, weeks_ago=3)
    db_session.add(recent_failure)
    await db_session.flush()

    cutoff = datetime.now(UTC) - timedelta(weeks=8)
    deleted = await delete_old_accept_failures(db_session, cutoff)

    assert deleted == 0

    from sqlalchemy import select

    result = await db_session.execute(
        select(AcceptFailure).where(AcceptFailure.id == recent_failure.id)
    )
    assert result.scalar_one_or_none() is not None


@pytest.mark.asyncio
async def test_no_records_at_all(db_session):
    """Empty tables — should return 0 deleted for both."""
    cutoff = datetime.now(UTC) - timedelta(weeks=8)

    deleted_rides = await delete_old_rides(db_session, cutoff)
    assert deleted_rides == 0

    deleted_failures = await delete_old_accept_failures(db_session, cutoff)
    assert deleted_failures == 0


# ---------------------------------------------------------------------------
# Batch deletion — integration tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_batch_deletion_rides(db_session):
    """Batch deletion should handle more records than BATCH_SIZE correctly."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    rides = [_make_ride(user, weeks_ago=10) for _ in range(5)]
    db_session.add_all(rides)
    await db_session.flush()

    cutoff = datetime.now(UTC) - timedelta(weeks=8)

    with patch("app.tasks.data_cleanup.BATCH_SIZE", 2):
        deleted = await delete_old_rides(db_session, cutoff)

    assert deleted == 5

    from sqlalchemy import select

    result = await db_session.execute(select(Ride))
    remaining = result.scalars().all()
    assert len(remaining) == 0


@pytest.mark.asyncio
async def test_batch_deletion_accept_failures(db_session):
    """Batch deletion should handle more accept failures than BATCH_SIZE."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    failures = [_make_accept_failure(user, weeks_ago=12) for _ in range(5)]
    db_session.add_all(failures)
    await db_session.flush()

    cutoff = datetime.now(UTC) - timedelta(weeks=8)

    with patch("app.tasks.data_cleanup.BATCH_SIZE", 2):
        deleted = await delete_old_accept_failures(db_session, cutoff)

    assert deleted == 5

    from sqlalchemy import select

    result = await db_session.execute(select(AcceptFailure))
    remaining = result.scalars().all()
    assert len(remaining) == 0


# ---------------------------------------------------------------------------
# cleanup_old_data — unit tests (mocked DB + sleep)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cleanup_loop_calls_delete_functions():
    """The loop should call both delete functions on each iteration."""
    mock_db = AsyncMock()

    @asynccontextmanager
    async def mock_session_factory():
        yield mock_db

    call_count = 0

    with (
        patch(
            "app.tasks.data_cleanup.AsyncSessionLocal",
            side_effect=mock_session_factory,
        ),
        patch(
            "app.tasks.data_cleanup.clear_ride_reference_ids",
            new_callable=AsyncMock,
            return_value=1,
        ) as mock_clear_refs,
        patch(
            "app.tasks.data_cleanup.delete_old_rides",
            new_callable=AsyncMock,
            return_value=3,
        ) as mock_del_rides,
        patch(
            "app.tasks.data_cleanup.delete_old_accept_failures",
            new_callable=AsyncMock,
            return_value=2,
        ) as mock_del_failures,
        patch(
            "app.tasks.data_cleanup.asyncio.sleep",
            new_callable=AsyncMock,
        ) as mock_sleep,
    ):

        async def sleep_side_effect(seconds):
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                # Second sleep (after work iteration) — stop the loop
                raise asyncio.CancelledError()

        mock_sleep.side_effect = sleep_side_effect

        with pytest.raises(asyncio.CancelledError):
            await cleanup_old_data()

        mock_clear_refs.assert_called_once()
        mock_del_rides.assert_called_once()
        mock_del_failures.assert_called_once()

        # Verify cutoff is approximately 8 weeks ago
        ride_call_args = mock_del_rides.call_args
        assert ride_call_args[0][0] is mock_db
        cutoff = ride_call_args[0][1]
        expected = datetime.now(UTC) - timedelta(weeks=8)
        assert abs((cutoff - expected).total_seconds()) < 5


@pytest.mark.asyncio
async def test_cleanup_continues_after_db_error():
    """The loop should catch exceptions and continue (not crash)."""
    call_count = 0

    @asynccontextmanager
    async def mock_session_factory():
        raise OperationalError("SELECT 1", {}, Exception("DB connection failed"))
        yield  # pragma: no cover

    with (
        patch(
            "app.tasks.data_cleanup.AsyncSessionLocal",
            side_effect=mock_session_factory,
        ),
        patch(
            "app.tasks.data_cleanup.asyncio.sleep",
            new_callable=AsyncMock,
        ) as mock_sleep,
    ):

        async def sleep_side_effect(seconds):
            nonlocal call_count
            call_count += 1
            if call_count >= 3:
                # Initial delay + 2 work iterations — stop after surviving 2 errors
                raise asyncio.CancelledError()

        mock_sleep.side_effect = sleep_side_effect

        with pytest.raises(asyncio.CancelledError):
            await cleanup_old_data()

        # Should have survived: initial sleep(3600) + sleep after error #1 + sleep after error #2
        assert call_count == 3


@pytest.mark.asyncio
async def test_cleanup_uses_correct_interval_and_delay():
    """The initial delay should be 3600s, and work interval should be 86400s."""
    mock_db = AsyncMock()

    @asynccontextmanager
    async def mock_session_factory():
        yield mock_db

    sleep_calls = []

    with (
        patch(
            "app.tasks.data_cleanup.AsyncSessionLocal",
            side_effect=mock_session_factory,
        ),
        patch(
            "app.tasks.data_cleanup.clear_ride_reference_ids",
            new_callable=AsyncMock,
            return_value=0,
        ),
        patch(
            "app.tasks.data_cleanup.delete_old_rides",
            new_callable=AsyncMock,
            return_value=0,
        ),
        patch(
            "app.tasks.data_cleanup.delete_old_accept_failures",
            new_callable=AsyncMock,
            return_value=0,
        ),
        patch(
            "app.tasks.data_cleanup.asyncio.sleep",
            new_callable=AsyncMock,
        ) as mock_sleep,
    ):

        async def sleep_side_effect(seconds):
            sleep_calls.append(seconds)
            if len(sleep_calls) >= 2:
                raise asyncio.CancelledError()

        mock_sleep.side_effect = sleep_side_effect

        with pytest.raises(asyncio.CancelledError):
            await cleanup_old_data()

        # First call: initial delay (3600s), second call: cleanup interval (86400s)
        assert sleep_calls[0] == 3600
        assert sleep_calls[1] == 86400


# ---------------------------------------------------------------------------
# clear_ride_reference_ids — integration tests (real DB)
# ---------------------------------------------------------------------------


def _make_credit_transaction(
    user: User,
    *,
    txn_type: str = "RIDE_CHARGE",
    amount: int = -2,
    balance_after: int = 8,
    reference_id: uuid.UUID | None = None,
) -> CreditTransaction:
    """Create a CreditTransaction for testing."""
    return CreditTransaction(
        id=uuid.uuid4(),
        user_id=user.id,
        type=txn_type,
        amount=amount,
        balance_after=balance_after,
        reference_id=reference_id,
    )


@pytest.mark.asyncio
async def test_clears_reference_ids_before_ride_deletion(db_session):
    """reference_id set to NULL for old rides; credit_transaction preserved after deletion."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    old_ride = _make_ride(user, weeks_ago=9)
    db_session.add(old_ride)
    await db_session.flush()

    txn = _make_credit_transaction(user, reference_id=old_ride.id)
    db_session.add(txn)
    await db_session.flush()

    cutoff = datetime.now(UTC) - timedelta(weeks=8)

    # Step 1: Clear reference_ids
    cleared = await clear_ride_reference_ids(db_session, cutoff)
    assert cleared == 1

    # Step 2: Delete old rides
    deleted = await delete_old_rides(db_session, cutoff)
    assert deleted == 1

    from sqlalchemy import select

    # credit_transaction still exists with reference_id = NULL
    result = await db_session.execute(
        select(CreditTransaction).where(CreditTransaction.id == txn.id)
    )
    remaining_txn = result.scalar_one_or_none()
    assert remaining_txn is not None
    assert remaining_txn.reference_id is None
    assert remaining_txn.amount == -2
    assert remaining_txn.balance_after == 8

    # ride is deleted
    result = await db_session.execute(select(Ride).where(Ride.id == old_ride.id))
    assert result.scalar_one_or_none() is None


@pytest.mark.asyncio
async def test_does_not_clear_reference_ids_for_recent_rides(db_session):
    """reference_id untouched for rides newer than cutoff."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    recent_ride = _make_ride(user, weeks_ago=4)
    db_session.add(recent_ride)
    await db_session.flush()

    txn = _make_credit_transaction(user, amount=-1, balance_after=9, reference_id=recent_ride.id)
    db_session.add(txn)
    await db_session.flush()

    cutoff = datetime.now(UTC) - timedelta(weeks=8)
    cleared = await clear_ride_reference_ids(db_session, cutoff)
    assert cleared == 0

    from sqlalchemy import select

    result = await db_session.execute(
        select(CreditTransaction).where(CreditTransaction.id == txn.id)
    )
    remaining_txn = result.scalar_one_or_none()
    assert remaining_txn is not None
    assert remaining_txn.reference_id == recent_ride.id


@pytest.mark.asyncio
async def test_clears_both_ride_charge_and_ride_refund(db_session):
    """Both RIDE_CHARGE and RIDE_REFUND reference_ids are cleared for old rides."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    old_ride = _make_ride(user, weeks_ago=10)
    db_session.add(old_ride)
    await db_session.flush()

    charge_txn = _make_credit_transaction(
        user, txn_type="RIDE_CHARGE", amount=-2, balance_after=8, reference_id=old_ride.id
    )
    refund_txn = _make_credit_transaction(
        user, txn_type="RIDE_REFUND", amount=2, balance_after=10, reference_id=old_ride.id
    )
    db_session.add_all([charge_txn, refund_txn])
    await db_session.flush()

    cutoff = datetime.now(UTC) - timedelta(weeks=8)
    cleared = await clear_ride_reference_ids(db_session, cutoff)
    assert cleared == 2

    from sqlalchemy import select

    for txn_id in (charge_txn.id, refund_txn.id):
        result = await db_session.execute(
            select(CreditTransaction).where(CreditTransaction.id == txn_id)
        )
        remaining = result.scalar_one()
        assert remaining.reference_id is None


@pytest.mark.asyncio
async def test_no_reference_ids_to_clear(db_session):
    """Returns 0 when no credit_transactions reference old rides."""
    cutoff = datetime.now(UTC) - timedelta(weeks=8)
    cleared = await clear_ride_reference_ids(db_session, cutoff)
    assert cleared == 0


@pytest.mark.asyncio
async def test_cleanup_does_not_affect_purchase_orders(db_session):
    """PURCHASE transactions with reference_id pointing to purchase_order.id stay intact."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    # Create a purchase order and a PURCHASE transaction referencing it
    purchase = PurchaseOrder(
        id=uuid.uuid4(),
        user_id=user.id,
        product_id="credits_50",
        purchase_token=str(uuid.uuid4()),
        credits_amount=50,
        status="VERIFIED",
    )
    db_session.add(purchase)
    await db_session.flush()

    purchase_txn = _make_credit_transaction(
        user,
        txn_type="PURCHASE",
        amount=50,
        balance_after=60,
        reference_id=purchase.id,
    )
    db_session.add(purchase_txn)

    # Also create an old ride to trigger cleanup
    old_ride = _make_ride(user, weeks_ago=9)
    db_session.add(old_ride)

    ride_txn = _make_credit_transaction(
        user,
        txn_type="RIDE_CHARGE",
        amount=-2,
        balance_after=58,
        reference_id=old_ride.id,
    )
    db_session.add(ride_txn)
    await db_session.flush()

    cutoff = datetime.now(UTC) - timedelta(weeks=8)

    # Clear ride reference_ids and delete old rides
    cleared = await clear_ride_reference_ids(db_session, cutoff)
    assert cleared == 1  # only the ride transaction

    deleted = await delete_old_rides(db_session, cutoff)
    assert deleted == 1

    from sqlalchemy import select

    # PURCHASE transaction reference_id must remain unchanged
    result = await db_session.execute(
        select(CreditTransaction).where(CreditTransaction.id == purchase_txn.id)
    )
    remaining_purchase_txn = result.scalar_one()
    assert remaining_purchase_txn.reference_id == purchase.id
    assert remaining_purchase_txn.amount == 50
    assert remaining_purchase_txn.type == "PURCHASE"

    # RIDE_CHARGE transaction reference_id must be NULL
    result = await db_session.execute(
        select(CreditTransaction).where(CreditTransaction.id == ride_txn.id)
    )
    remaining_ride_txn = result.scalar_one()
    assert remaining_ride_txn.reference_id is None

    # Purchase order itself must still exist
    result = await db_session.execute(select(PurchaseOrder).where(PurchaseOrder.id == purchase.id))
    assert result.scalar_one_or_none() is not None


# ---------------------------------------------------------------------------
# Atomicity — rides not deleted when reference clearing fails
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_rides_not_deleted_when_reference_clearing_fails():
    """If clear_ride_reference_ids fails, delete_old_rides must NOT run."""
    mock_db = AsyncMock()

    @asynccontextmanager
    async def mock_session_factory():
        yield mock_db

    call_count = 0

    with (
        patch(
            "app.tasks.data_cleanup.AsyncSessionLocal",
            side_effect=mock_session_factory,
        ),
        patch(
            "app.tasks.data_cleanup.clear_ride_reference_ids",
            new_callable=AsyncMock,
            side_effect=OperationalError("UPDATE", {}, Exception("DB error during UPDATE")),
        ) as mock_clear_refs,
        patch(
            "app.tasks.data_cleanup.delete_old_rides",
            new_callable=AsyncMock,
        ) as mock_del_rides,
        patch(
            "app.tasks.data_cleanup.delete_old_accept_failures",
            new_callable=AsyncMock,
        ) as mock_del_failures,
        patch(
            "app.tasks.data_cleanup.asyncio.sleep",
            new_callable=AsyncMock,
        ) as mock_sleep,
    ):

        async def sleep_side_effect(seconds):
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                raise asyncio.CancelledError()

        mock_sleep.side_effect = sleep_side_effect

        with pytest.raises(asyncio.CancelledError):
            await cleanup_old_data()

        mock_clear_refs.assert_called_once()
        mock_del_rides.assert_not_called()
        mock_del_failures.assert_not_called()


# ---------------------------------------------------------------------------
# Logging — verify cleared reference count in log
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cleanup_logs_cleared_references(caplog):
    """Log should include count of cleared references, deleted rides and failures."""
    mock_db = AsyncMock()

    @asynccontextmanager
    async def mock_session_factory():
        yield mock_db

    call_count = 0

    with (
        patch(
            "app.tasks.data_cleanup.AsyncSessionLocal",
            side_effect=mock_session_factory,
        ),
        patch(
            "app.tasks.data_cleanup.clear_ride_reference_ids",
            new_callable=AsyncMock,
            return_value=5,
        ),
        patch(
            "app.tasks.data_cleanup.delete_old_rides",
            new_callable=AsyncMock,
            return_value=3,
        ),
        patch(
            "app.tasks.data_cleanup.delete_old_accept_failures",
            new_callable=AsyncMock,
            return_value=2,
        ),
        patch(
            "app.tasks.data_cleanup.asyncio.sleep",
            new_callable=AsyncMock,
        ) as mock_sleep,
    ):

        async def sleep_side_effect(seconds):
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                raise asyncio.CancelledError()

        mock_sleep.side_effect = sleep_side_effect

        with (
            caplog.at_level(logging.INFO, logger="app.tasks.data_cleanup"),
            pytest.raises(asyncio.CancelledError),
        ):
            await cleanup_old_data()

        assert "cleared 5 reference(s)" in caplog.text
        assert "deleted 3 ride(s)" in caplog.text
        assert "2 accept failure(s)" in caplog.text
