"""Tests for app.tasks.balance_reconciliation (task 8.4).

Test strategy:
1. Balance matches SUM(transactions) -> no log
2. Balance does NOT match -> BALANCE_MISMATCH logged
3. Ride credits_charged mismatch -> RIDE_CREDIT_MISMATCH logged
4. Ride credits_refunded mismatch -> RIDE_CREDIT_MISMATCH logged
5. Rides older than 8 weeks NOT checked (reference_id already NULL)
6. ALL rides checked (not just CONFIRMED)
7. Checkpoint: interrupted task continues from last checkpoint
8. Batch processing: 150 users -> 3 batches with pauses
9. Full reconciliation weekly: after 7 days checkpoint ignored
10. Full reconciliation on mismatch: BALANCE_MISMATCH -> checkpoint reset
11. Graceful shutdown: checkpoint saved on interrupt
12. Mass checkpoint loss (Redis restart) -> batches with pauses, no spike
"""

import asyncio
import json
import uuid
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy.exc import OperationalError

from app.models.credit_balance import CreditBalance
from app.models.credit_transaction import CreditTransaction, TransactionType
from app.models.ride import Ride
from app.models.user import User
from app.tasks.balance_reconciliation import (
    LAST_FULL_RUN_KEY,
    RECONCILIATION_INTERVAL_SECONDS,
    checkpoint_key,
    get_all_user_ids,
    get_checkpoint,
    mark_full_run,
    needs_full_run,
    reconcile_ride_credits,
    reconcile_user_balance,
    run_balance_reconciliation,
    save_checkpoint,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_user(email: str = "test@example.com") -> User:
    return User(
        id=uuid.uuid4(),
        email=email,
        password_hash="fakehash",
    )


def _make_balance(user: User, balance: int) -> CreditBalance:
    return CreditBalance(
        id=uuid.uuid4(),
        user_id=user.id,
        balance=balance,
    )


def _make_transaction(
    user: User,
    tx_type: str,
    amount: int,
    balance_after: int,
    reference_id: uuid.UUID | None = None,
    created_at: datetime | None = None,
) -> CreditTransaction:
    tx = CreditTransaction(
        id=uuid.uuid4(),
        user_id=user.id,
        type=tx_type,
        amount=amount,
        balance_after=balance_after,
        reference_id=reference_id,
    )
    if created_at is not None:
        tx.created_at = created_at
    return tx


def _make_ride(
    user: User,
    credits_charged: int = 0,
    credits_refunded: int = 0,
    verification_status: str = "PENDING",
    created_at: datetime | None = None,
) -> Ride:
    ride = Ride(
        id=uuid.uuid4(),
        user_id=user.id,
        idempotency_key=str(uuid.uuid4()),
        event_type="ACCEPTED",
        ride_data={"price": 25.0, "pickup_time": "Tomorrow 6:05AM"},
        ride_hash="a" * 64,
        verification_status=verification_status,
        credits_charged=credits_charged,
        credits_refunded=credits_refunded,
    )
    if created_at is not None:
        ride.created_at = created_at
    return ride


# ---------------------------------------------------------------------------
# 1. Balance matches SUM(transactions) -> no log
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_balance_matches_no_mismatch_logged(db_session, fake_redis, caplog):
    """When balance == SUM(transactions), no BALANCE_MISMATCH is logged."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    db_session.add(_make_balance(user, 10))
    db_session.add(_make_transaction(user, TransactionType.REGISTRATION_BONUS.value, 10, 10))
    await db_session.flush()

    result = await reconcile_user_balance(user.id, True, db_session, fake_redis)

    assert result is True
    assert "BALANCE_MISMATCH" not in caplog.text


# ---------------------------------------------------------------------------
# 2. Balance does NOT match -> BALANCE_MISMATCH logged
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_balance_mismatch_logged(db_session, fake_redis, caplog):
    """When balance != SUM(transactions), BALANCE_MISMATCH is logged."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    # Balance says 15, but only one +10 transaction
    db_session.add(_make_balance(user, 15))
    db_session.add(_make_transaction(user, TransactionType.REGISTRATION_BONUS.value, 10, 10))
    await db_session.flush()

    result = await reconcile_user_balance(user.id, True, db_session, fake_redis)

    assert result is False
    assert "BALANCE_MISMATCH" in caplog.text
    assert str(user.id) in caplog.text
    assert "expected=10" in caplog.text
    assert "actual=15" in caplog.text
    assert "diff=5" in caplog.text


# ---------------------------------------------------------------------------
# 3. Ride credits_charged mismatch -> RIDE_CREDIT_MISMATCH logged
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ride_credits_charged_mismatch(db_session, caplog):
    """credits_charged on ride != |amount| of RIDE_CHARGE tx -> logged."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    ride = _make_ride(user, credits_charged=3)
    db_session.add(ride)
    await db_session.flush()

    # Transaction says -2, but ride says credits_charged=3
    db_session.add(
        _make_transaction(
            user,
            TransactionType.RIDE_CHARGE.value,
            -2,
            8,
            reference_id=ride.id,
        )
    )
    await db_session.flush()

    await reconcile_ride_credits(user.id, db_session)

    assert "RIDE_CREDIT_MISMATCH" in caplog.text
    assert "credits_charged" in caplog.text
    assert str(ride.id) in caplog.text


# ---------------------------------------------------------------------------
# 4. Ride credits_refunded mismatch -> RIDE_CREDIT_MISMATCH logged
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ride_credits_refunded_mismatch(db_session, caplog):
    """credits_refunded on ride != amount of RIDE_REFUND tx -> logged."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    ride = _make_ride(
        user,
        credits_charged=2,
        credits_refunded=2,
        verification_status="CANCELLED",
    )
    db_session.add(ride)
    await db_session.flush()

    # RIDE_CHARGE matches
    db_session.add(
        _make_transaction(user, TransactionType.RIDE_CHARGE.value, -2, 8, reference_id=ride.id)
    )
    # RIDE_REFUND says +1, but ride says credits_refunded=2
    db_session.add(
        _make_transaction(user, TransactionType.RIDE_REFUND.value, 1, 9, reference_id=ride.id)
    )
    await db_session.flush()

    await reconcile_ride_credits(user.id, db_session)

    assert "RIDE_CREDIT_MISMATCH" in caplog.text
    assert "credits_refunded" in caplog.text
    assert str(ride.id) in caplog.text


# ---------------------------------------------------------------------------
# 5. Rides older than 8 weeks NOT checked
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_old_rides_not_checked(db_session, caplog):
    """Rides older than 8 weeks should NOT be checked (reference_id is NULL)."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    old_time = datetime.now(UTC) - timedelta(weeks=9)
    ride = _make_ride(user, credits_charged=2, created_at=old_time)
    db_session.add(ride)
    await db_session.flush()

    # No matching RIDE_CHARGE transaction — would be a mismatch if checked
    await reconcile_ride_credits(user.id, db_session)

    assert "RIDE_CREDIT_MISMATCH" not in caplog.text


# ---------------------------------------------------------------------------
# 6. ALL rides checked (not just CONFIRMED)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_all_verification_statuses_checked(db_session, caplog):
    """Rides with any verification_status are checked, not just CONFIRMED."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    for status in ("PENDING", "CONFIRMED", "CANCELLED"):
        ride = _make_ride(user, credits_charged=2, verification_status=status)
        db_session.add(ride)
        await db_session.flush()
        # No RIDE_CHARGE transaction -> mismatch for each

    await reconcile_ride_credits(user.id, db_session)

    # Should have 3 RIDE_CREDIT_MISMATCH warnings (one per ride)
    mismatch_count = caplog.text.count("RIDE_CREDIT_MISMATCH")
    assert mismatch_count == 3


# ---------------------------------------------------------------------------
# 7. Checkpoint: interrupted task continues from last checkpoint
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_checkpoint_incremental_reconciliation(db_session, fake_redis):
    """After saving checkpoint, next run only checks new transactions."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    db_session.add(_make_balance(user, 15))

    old_time = datetime.now(UTC) - timedelta(hours=2)
    tx1 = _make_transaction(
        user,
        TransactionType.REGISTRATION_BONUS.value,
        10,
        10,
        created_at=old_time,
    )
    db_session.add(tx1)
    await db_session.flush()

    # Save checkpoint after first transaction
    await save_checkpoint(
        user.id,
        str(tx1.id),
        tx1.created_at.isoformat(),
        10,
        fake_redis,
    )

    # Add a newer transaction
    tx2 = _make_transaction(user, TransactionType.PURCHASE.value, 5, 15)
    db_session.add(tx2)
    await db_session.flush()

    # Incremental reconciliation: checkpoint(10) + new(5) = 15 == actual(15)
    result = await reconcile_user_balance(user.id, False, db_session, fake_redis)
    assert result is True


@pytest.mark.asyncio
async def test_checkpoint_incremental_detects_mismatch(db_session, fake_redis, caplog):
    """Incremental reconciliation detects mismatch from checkpoint."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    # Balance is 20, but checkpoint(10) + new tx(5) = 15
    db_session.add(_make_balance(user, 20))

    old_time = datetime.now(UTC) - timedelta(hours=2)
    tx1 = _make_transaction(
        user,
        TransactionType.REGISTRATION_BONUS.value,
        10,
        10,
        created_at=old_time,
    )
    db_session.add(tx1)
    await db_session.flush()

    await save_checkpoint(
        user.id,
        str(tx1.id),
        tx1.created_at.isoformat(),
        10,
        fake_redis,
    )

    tx2 = _make_transaction(user, TransactionType.PURCHASE.value, 5, 15)
    db_session.add(tx2)
    await db_session.flush()

    result = await reconcile_user_balance(user.id, False, db_session, fake_redis)
    assert result is False
    assert "BALANCE_MISMATCH" in caplog.text


# ---------------------------------------------------------------------------
# 8. Batch processing: 150 users -> 3 batches with pauses
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_batch_processing_pauses_between_batches():
    """150 users processed in ceil(150/50)=3 batches with pauses between."""
    user_ids = [uuid.uuid4() for _ in range(150)]
    mock_db = AsyncMock()

    @asynccontextmanager
    async def mock_session():
        yield mock_db

    sleep_durations: list[float] = []

    with (
        patch(
            "app.tasks.balance_reconciliation.AsyncSessionLocal",
            side_effect=mock_session,
        ),
        patch(
            "app.tasks.balance_reconciliation.needs_full_run",
            new_callable=AsyncMock,
            return_value=False,
        ),
        patch(
            "app.tasks.balance_reconciliation.get_all_user_ids",
            new_callable=AsyncMock,
            return_value=user_ids,
        ),
        patch(
            "app.tasks.balance_reconciliation.reconcile_user_balance",
            new_callable=AsyncMock,
            return_value=True,
        ) as mock_reconcile,
        patch(
            "app.tasks.balance_reconciliation.reconcile_ride_credits",
            new_callable=AsyncMock,
        ),
        patch(
            "app.tasks.balance_reconciliation.asyncio.sleep",
            new_callable=AsyncMock,
        ) as mock_sleep,
        patch(
            "app.tasks.balance_reconciliation.redis_client",
            new_callable=AsyncMock,
        ),
    ):

        async def sleep_handler(seconds):
            sleep_durations.append(seconds)
            # Stop after the first complete iteration (interval sleep)
            if seconds == RECONCILIATION_INTERVAL_SECONDS:
                raise asyncio.CancelledError()

        mock_sleep.side_effect = sleep_handler

        with pytest.raises(asyncio.CancelledError):
            await run_balance_reconciliation()

        # All 150 users should be reconciled exactly once
        assert mock_reconcile.call_count == 150

        # Between-batch pauses: 2 pauses for 3 batches (after batch 1 and 2)
        batch_pauses = [d for d in sleep_durations if d == 0.1]
        assert len(batch_pauses) == 2


# ---------------------------------------------------------------------------
# 9. Full reconciliation weekly: after 7 days checkpoint ignored
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_full_run_after_7_days(fake_redis):
    """needs_full_run returns True when last full run was > 7 days ago."""
    old_time = (datetime.now(UTC) - timedelta(days=8)).isoformat()
    fake_redis._store[LAST_FULL_RUN_KEY] = old_time

    assert await needs_full_run(fake_redis) is True


@pytest.mark.asyncio
async def test_no_full_run_within_7_days(fake_redis):
    """needs_full_run returns False when last full run was < 7 days ago."""
    recent_time = (datetime.now(UTC) - timedelta(days=3)).isoformat()
    fake_redis._store[LAST_FULL_RUN_KEY] = recent_time

    assert await needs_full_run(fake_redis) is False


@pytest.mark.asyncio
async def test_full_run_when_key_missing(fake_redis):
    """needs_full_run returns True when Redis key is missing."""
    assert await needs_full_run(fake_redis) is True


@pytest.mark.asyncio
async def test_full_run_ignores_checkpoint(db_session, fake_redis):
    """When force_full=True, checkpoint is ignored and full SUM is used."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    db_session.add(_make_balance(user, 10))

    tx = _make_transaction(user, TransactionType.REGISTRATION_BONUS.value, 10, 10)
    db_session.add(tx)
    await db_session.flush()

    # Save a stale checkpoint with wrong balance
    await save_checkpoint(user.id, str(tx.id), tx.created_at.isoformat(), 999, fake_redis)

    # force_full=True -> ignores checkpoint, uses full SUM(10) == balance(10)
    result = await reconcile_user_balance(user.id, True, db_session, fake_redis)
    assert result is True


# ---------------------------------------------------------------------------
# 10. Full reconciliation on mismatch: checkpoint reset
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_mismatch_deletes_checkpoint(db_session, fake_redis):
    """On BALANCE_MISMATCH, checkpoint is deleted for this user."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    db_session.add(_make_balance(user, 99))
    db_session.add(_make_transaction(user, TransactionType.REGISTRATION_BONUS.value, 10, 10))
    await db_session.flush()

    # Pre-set checkpoint
    key = checkpoint_key(user.id)
    fake_redis._store[key] = json.dumps(
        {
            "last_tx_id": str(uuid.uuid4()),
            "last_tx_created_at": datetime.now(UTC).isoformat(),
            "balance_at_checkpoint": 10,
        }
    )

    result = await reconcile_user_balance(user.id, True, db_session, fake_redis)
    assert result is False

    # Checkpoint should be deleted
    assert key not in fake_redis._store


# ---------------------------------------------------------------------------
# 11. Graceful shutdown: checkpoint saved on interrupt
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_checkpoint_saved_before_interrupt(db_session, fake_redis):
    """Checkpoint is saved per user, so interrupt preserves completed work."""
    user_a = _make_user("a@example.com")
    user_b = _make_user("b@example.com")
    db_session.add_all([user_a, user_b])
    await db_session.flush()

    # User A: balance=10, tx=+10 (matches)
    db_session.add(_make_balance(user_a, 10))
    tx_a = _make_transaction(user_a, TransactionType.REGISTRATION_BONUS.value, 10, 10)
    db_session.add(tx_a)

    # User B: balance=5, tx=+5 (matches)
    db_session.add(_make_balance(user_b, 5))
    tx_b = _make_transaction(user_b, TransactionType.REGISTRATION_BONUS.value, 5, 5)
    db_session.add(tx_b)
    await db_session.flush()

    # Reconcile user A — should save checkpoint
    result_a = await reconcile_user_balance(user_a.id, True, db_session, fake_redis)
    assert result_a is True
    assert checkpoint_key(user_a.id) in fake_redis._store

    # Simulate interrupt before user B is processed
    # User A's checkpoint is preserved, user B has no checkpoint
    assert checkpoint_key(user_b.id) not in fake_redis._store


# ---------------------------------------------------------------------------
# 12. Mass checkpoint loss (Redis restart) -> batches with pauses
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_mass_checkpoint_loss_triggers_full_run():
    """Redis restart loses all checkpoints -> needs_full_run returns True."""
    # Empty fake Redis (simulates restart)
    mock_redis = AsyncMock()
    mock_redis.get = AsyncMock(return_value=None)

    assert await needs_full_run(mock_redis) is True


@pytest.mark.asyncio
async def test_missing_checkpoint_causes_full_reconciliation(db_session, fake_redis):
    """User with no checkpoint gets full reconciliation (not incremental)."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    db_session.add(_make_balance(user, 10))
    db_session.add(_make_transaction(user, TransactionType.REGISTRATION_BONUS.value, 10, 10))
    await db_session.flush()

    # No checkpoint in Redis — full reconciliation
    result = await reconcile_user_balance(user.id, False, db_session, fake_redis)
    assert result is True


# ---------------------------------------------------------------------------
# Additional edge cases
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ride_no_charge_transaction_when_zero_charged(db_session, caplog):
    """Ride with credits_charged=0 and no RIDE_CHARGE tx -> no mismatch."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    ride = _make_ride(user, credits_charged=0)
    db_session.add(ride)
    await db_session.flush()

    await reconcile_ride_credits(user.id, db_session)

    assert "RIDE_CREDIT_MISMATCH" not in caplog.text


@pytest.mark.asyncio
async def test_ride_matching_credits(db_session, caplog):
    """Ride with matching credits_charged and credits_refunded -> no log."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    ride = _make_ride(
        user,
        credits_charged=2,
        credits_refunded=2,
        verification_status="CANCELLED",
    )
    db_session.add(ride)
    await db_session.flush()

    db_session.add(
        _make_transaction(user, TransactionType.RIDE_CHARGE.value, -2, 8, reference_id=ride.id)
    )
    db_session.add(
        _make_transaction(user, TransactionType.RIDE_REFUND.value, 2, 10, reference_id=ride.id)
    )
    await db_session.flush()

    await reconcile_ride_credits(user.id, db_session)

    assert "RIDE_CREDIT_MISMATCH" not in caplog.text


@pytest.mark.asyncio
async def test_no_users_no_error(db_session, fake_redis, caplog):
    """When no users have credit balances, task completes without error."""
    user_ids = await get_all_user_ids(db_session)
    assert user_ids == []


@pytest.mark.asyncio
async def test_checkpoint_saved_after_successful_match(db_session, fake_redis):
    """After a successful reconciliation, checkpoint is saved with latest tx."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    db_session.add(_make_balance(user, 10))
    tx = _make_transaction(user, TransactionType.REGISTRATION_BONUS.value, 10, 10)
    db_session.add(tx)
    await db_session.flush()

    await reconcile_user_balance(user.id, True, db_session, fake_redis)

    key = checkpoint_key(user.id)
    assert key in fake_redis._store
    cp = json.loads(fake_redis._store[key])
    assert cp["last_tx_id"] == str(tx.id)
    assert cp["balance_at_checkpoint"] == 10


@pytest.mark.asyncio
async def test_mark_full_run_persists(fake_redis):
    """mark_full_run saves timestamp to Redis."""
    await mark_full_run(fake_redis)
    assert LAST_FULL_RUN_KEY in fake_redis._store


@pytest.mark.asyncio
async def test_get_checkpoint_returns_none_on_corrupt_data(fake_redis):
    """Corrupt JSON in checkpoint returns None (graceful degradation)."""
    uid = uuid.uuid4()
    fake_redis._store[checkpoint_key(uid)] = "not-json"

    result = await get_checkpoint(uid, fake_redis)
    assert result is None


@pytest.mark.asyncio
async def test_get_checkpoint_returns_none_on_missing_fields(fake_redis):
    """Checkpoint with missing required fields returns None."""
    uid = uuid.uuid4()
    fake_redis._store[checkpoint_key(uid)] = json.dumps({"last_tx_id": "abc"})

    result = await get_checkpoint(uid, fake_redis)
    assert result is None


@pytest.mark.asyncio
async def test_redis_unavailable_on_checkpoint_get():
    """Redis failure on GET returns None for checkpoint."""
    from redis.exceptions import RedisError

    mock_redis = AsyncMock()
    mock_redis.get = AsyncMock(side_effect=RedisError("down"))

    result = await get_checkpoint(uuid.uuid4(), mock_redis)
    assert result is None


@pytest.mark.asyncio
async def test_redis_unavailable_on_needs_full_run():
    """Redis failure on needs_full_run returns True (safe default)."""
    from redis.exceptions import RedisError

    mock_redis = AsyncMock()
    mock_redis.get = AsyncMock(side_effect=RedisError("down"))

    assert await needs_full_run(mock_redis) is True


# ---------------------------------------------------------------------------
# run_balance_reconciliation — main loop tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_main_loop_processes_users_and_sleeps():
    """Main loop: fetch users, reconcile each, sleep for interval."""
    user_id = uuid.uuid4()
    mock_db = AsyncMock()

    @asynccontextmanager
    async def mock_session():
        yield mock_db

    call_count = 0

    with (
        patch(
            "app.tasks.balance_reconciliation.AsyncSessionLocal",
            side_effect=mock_session,
        ),
        patch(
            "app.tasks.balance_reconciliation.needs_full_run",
            new_callable=AsyncMock,
            return_value=False,
        ),
        patch(
            "app.tasks.balance_reconciliation.get_all_user_ids",
            new_callable=AsyncMock,
            return_value=[user_id],
        ),
        patch(
            "app.tasks.balance_reconciliation.reconcile_user_balance",
            new_callable=AsyncMock,
            return_value=True,
        ) as mock_reconcile,
        patch(
            "app.tasks.balance_reconciliation.reconcile_ride_credits",
            new_callable=AsyncMock,
        ) as mock_ride_reconcile,
        patch(
            "app.tasks.balance_reconciliation.asyncio.sleep",
            new_callable=AsyncMock,
        ) as mock_sleep,
        patch(
            "app.tasks.balance_reconciliation.redis_client",
            new_callable=AsyncMock,
        ),
    ):

        async def sleep_handler(seconds):
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                raise asyncio.CancelledError()

        mock_sleep.side_effect = sleep_handler

        with pytest.raises(asyncio.CancelledError):
            await run_balance_reconciliation()

        mock_reconcile.assert_called_once()
        mock_ride_reconcile.assert_called_once()


@pytest.mark.asyncio
async def test_main_loop_marks_full_run():
    """After full run, mark_full_run is called."""
    mock_db = AsyncMock()

    @asynccontextmanager
    async def mock_session():
        yield mock_db

    call_count = 0

    with (
        patch(
            "app.tasks.balance_reconciliation.AsyncSessionLocal",
            side_effect=mock_session,
        ),
        patch(
            "app.tasks.balance_reconciliation.needs_full_run",
            new_callable=AsyncMock,
            return_value=True,
        ),
        patch(
            "app.tasks.balance_reconciliation.get_all_user_ids",
            new_callable=AsyncMock,
            return_value=[uuid.uuid4()],
        ),
        patch(
            "app.tasks.balance_reconciliation.reconcile_user_balance",
            new_callable=AsyncMock,
            return_value=True,
        ),
        patch(
            "app.tasks.balance_reconciliation.reconcile_ride_credits",
            new_callable=AsyncMock,
        ),
        patch(
            "app.tasks.balance_reconciliation.mark_full_run",
            new_callable=AsyncMock,
        ) as mock_mark,
        patch(
            "app.tasks.balance_reconciliation.asyncio.sleep",
            new_callable=AsyncMock,
        ) as mock_sleep,
        patch(
            "app.tasks.balance_reconciliation.redis_client",
            new_callable=AsyncMock,
        ),
    ):

        async def sleep_handler(seconds):
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                raise asyncio.CancelledError()

        mock_sleep.side_effect = sleep_handler

        with pytest.raises(asyncio.CancelledError):
            await run_balance_reconciliation()

        mock_mark.assert_called_once()


@pytest.mark.asyncio
async def test_main_loop_survives_db_error():
    """DB error during main loop -> logged, task continues."""
    call_count = 0

    @asynccontextmanager
    async def mock_session():
        raise OperationalError("DB connection failed", {}, None)
        yield  # pragma: no cover

    with (
        patch(
            "app.tasks.balance_reconciliation.AsyncSessionLocal",
            side_effect=mock_session,
        ),
        patch(
            "app.tasks.balance_reconciliation.needs_full_run",
            new_callable=AsyncMock,
            return_value=False,
        ),
        patch(
            "app.tasks.balance_reconciliation.asyncio.sleep",
            new_callable=AsyncMock,
        ) as mock_sleep,
        patch(
            "app.tasks.balance_reconciliation.redis_client",
            new_callable=AsyncMock,
        ),
    ):

        async def sleep_handler(seconds):
            nonlocal call_count
            call_count += 1
            if call_count >= 3:
                raise asyncio.CancelledError()

        mock_sleep.side_effect = sleep_handler

        with pytest.raises(asyncio.CancelledError):
            await run_balance_reconciliation()

        # Should have survived the error and slept multiple times
        assert call_count == 3


@pytest.mark.asyncio
async def test_main_loop_per_user_error_doesnt_stop_others():
    """Error processing one user doesn't prevent processing of next user."""
    user_a = uuid.uuid4()
    user_b = uuid.uuid4()
    mock_db = AsyncMock()

    @asynccontextmanager
    async def mock_session():
        yield mock_db

    call_count = 0
    reconciled_users: list[uuid.UUID] = []

    with (
        patch(
            "app.tasks.balance_reconciliation.AsyncSessionLocal",
            side_effect=mock_session,
        ),
        patch(
            "app.tasks.balance_reconciliation.needs_full_run",
            new_callable=AsyncMock,
            return_value=False,
        ),
        patch(
            "app.tasks.balance_reconciliation.get_all_user_ids",
            new_callable=AsyncMock,
            return_value=[user_a, user_b],
        ),
        patch(
            "app.tasks.balance_reconciliation.reconcile_user_balance",
            new_callable=AsyncMock,
        ) as mock_reconcile,
        patch(
            "app.tasks.balance_reconciliation.reconcile_ride_credits",
            new_callable=AsyncMock,
        ),
        patch(
            "app.tasks.balance_reconciliation.asyncio.sleep",
            new_callable=AsyncMock,
        ) as mock_sleep,
        patch(
            "app.tasks.balance_reconciliation.redis_client",
            new_callable=AsyncMock,
        ),
    ):

        async def reconcile_side_effect(uid, force_full, db, redis):
            reconciled_users.append(uid)
            if uid == user_a:
                raise OperationalError("DB error for user A", {}, None)
            return True

        mock_reconcile.side_effect = reconcile_side_effect

        async def sleep_handler(seconds):
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                raise asyncio.CancelledError()

        mock_sleep.side_effect = sleep_handler

        with pytest.raises(asyncio.CancelledError):
            await run_balance_reconciliation()

        # Both users should have been attempted
        assert user_a in reconciled_users
        assert user_b in reconciled_users
