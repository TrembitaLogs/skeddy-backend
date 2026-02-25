"""Tests for ride_verification fallback background task (task 8.1).

Test strategy:
1. PENDING rides with expired deadline → status changed via fallback
2. last_reported_present=True → CONFIRMED (credits kept)
3. last_reported_present=False → CANCELLED + refund + FCM + cache
4. Double-processing protection: second run → nothing to process
5. Empty result (no expired rides) → graceful skip
"""

import asyncio
import uuid
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.credit_balance import CreditBalance
from app.models.credit_transaction import CreditTransaction, TransactionType
from app.models.ride import Ride
from app.models.user import User
from app.tasks.ride_verification import (
    get_users_with_expired_rides,
    process_user_verifications,
    run_verification_fallback,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _create_user(db: AsyncSession, *, fcm_token: str | None = None) -> User:
    user = User(
        email=f"verify-task-{uuid.uuid4().hex[:8]}@example.com",
        password_hash="hashed",
        fcm_token=fcm_token,
    )
    db.add(user)
    await db.flush()
    return user


async def _create_balance(
    db: AsyncSession, user_id: uuid.UUID, balance: int = 10
) -> CreditBalance:
    cb = CreditBalance(user_id=user_id, balance=balance)
    db.add(cb)
    await db.flush()
    return cb


async def _create_ride(
    db: AsyncSession,
    user_id: uuid.UUID,
    *,
    verification_status: str = "PENDING",
    verification_deadline: datetime | None = None,
    last_reported_present: bool | None = None,
    credits_charged: int = 2,
) -> Ride:
    if verification_deadline is None:
        verification_deadline = datetime.now(UTC) - timedelta(hours=1)
    ride = Ride(
        user_id=user_id,
        idempotency_key=str(uuid.uuid4()),
        event_type="ACCEPTED",
        ride_data={"price": 25.0, "pickup_time": "Tomorrow · 6:05AM"},
        ride_hash=uuid.uuid4().hex + uuid.uuid4().hex,
        verification_status=verification_status,
        verification_deadline=verification_deadline,
        last_reported_present=last_reported_present,
        credits_charged=credits_charged,
    )
    db.add(ride)
    await db.flush()
    return ride


async def _reload_ride(db: AsyncSession, ride_id: uuid.UUID) -> Ride:
    result = await db.execute(
        select(Ride).where(Ride.id == ride_id).execution_options(populate_existing=True)
    )
    return result.scalar_one()


async def _reload_balance(db: AsyncSession, user_id: uuid.UUID) -> int:
    result = await db.execute(
        select(CreditBalance.balance).where(CreditBalance.user_id == user_id)
    )
    return result.scalar_one()


# ---------------------------------------------------------------------------
# get_users_with_expired_rides — integration tests
# ---------------------------------------------------------------------------


class TestGetUsersWithExpiredRides:
    @pytest.mark.asyncio
    async def test_returns_users_with_expired_pending_rides(self, db_session):
        """Users with PENDING rides past deadline are returned."""
        user = await _create_user(db_session)
        await _create_ride(db_session, user.id, last_reported_present=True)

        user_ids = await get_users_with_expired_rides(db_session)
        assert user.id in user_ids

    @pytest.mark.asyncio
    async def test_excludes_non_pending_rides(self, db_session):
        """CONFIRMED/CANCELLED rides are NOT included."""
        user = await _create_user(db_session)
        await _create_ride(db_session, user.id, verification_status="CONFIRMED")
        await _create_ride(db_session, user.id, verification_status="CANCELLED")

        user_ids = await get_users_with_expired_rides(db_session)
        assert user_ids == []

    @pytest.mark.asyncio
    async def test_excludes_future_deadline(self, db_session):
        """PENDING rides with future deadline are NOT included."""
        user = await _create_user(db_session)
        future = datetime.now(UTC) + timedelta(hours=2)
        await _create_ride(db_session, user.id, verification_deadline=future)

        user_ids = await get_users_with_expired_rides(db_session)
        assert user_ids == []

    @pytest.mark.asyncio
    async def test_returns_distinct_users(self, db_session):
        """Multiple expired rides for same user → user_id appears once."""
        user = await _create_user(db_session)
        await _create_ride(db_session, user.id, last_reported_present=True)
        await _create_ride(db_session, user.id, last_reported_present=False)

        user_ids = await get_users_with_expired_rides(db_session)
        assert user_ids.count(user.id) == 1

    @pytest.mark.asyncio
    async def test_multiple_users(self, db_session):
        """Expired rides from different users → all user_ids returned."""
        user_a = await _create_user(db_session)
        user_b = await _create_user(db_session)
        await _create_ride(db_session, user_a.id, last_reported_present=True)
        await _create_ride(db_session, user_b.id, last_reported_present=False)

        user_ids = await get_users_with_expired_rides(db_session)
        assert set(user_ids) == {user_a.id, user_b.id}

    @pytest.mark.asyncio
    async def test_empty_when_no_rides(self, db_session):
        """No rides at all → empty list."""
        user_ids = await get_users_with_expired_rides(db_session)
        assert user_ids == []


# ---------------------------------------------------------------------------
# process_user_verifications — integration tests
# ---------------------------------------------------------------------------


class TestProcessUserVerifications:
    @pytest.mark.asyncio
    async def test_confirmed_when_present_true(self, db_session, fake_redis):
        """last_reported_present=True → CONFIRMED, no refund, no FCM."""
        user = await _create_user(db_session, fcm_token="tok")
        await _create_balance(db_session, user.id, balance=10)
        ride = await _create_ride(
            db_session,
            user.id,
            last_reported_present=True,
            credits_charged=2,
        )

        with patch(
            "app.tasks.ride_verification.send_ride_credit_refunded",
            new_callable=AsyncMock,
        ) as mock_fcm:
            result = await process_user_verifications(user.id, db_session, fake_redis)

        ride = await _reload_ride(db_session, ride.id)
        assert ride.verification_status == "CONFIRMED"
        assert ride.verified_at is not None
        assert ride.credits_refunded == 0
        assert result == []
        mock_fcm.assert_not_called()

        balance = await _reload_balance(db_session, user.id)
        assert balance == 10  # unchanged

    @pytest.mark.asyncio
    async def test_confirmed_when_present_null(self, db_session, fake_redis):
        """last_reported_present=NULL (offline) → CONFIRMED (presumption)."""
        user = await _create_user(db_session)
        await _create_balance(db_session, user.id, balance=10)
        ride = await _create_ride(
            db_session,
            user.id,
            last_reported_present=None,
            credits_charged=2,
        )

        with patch(
            "app.tasks.ride_verification.send_ride_credit_refunded",
            new_callable=AsyncMock,
        ):
            result = await process_user_verifications(user.id, db_session, fake_redis)

        ride = await _reload_ride(db_session, ride.id)
        assert ride.verification_status == "CONFIRMED"
        assert result == []

    @pytest.mark.asyncio
    async def test_cancelled_refund_fcm_cache(self, db_session, fake_redis):
        """last_reported_present=False → CANCELLED + refund + FCM + cache."""
        user = await _create_user(db_session, fcm_token="fcm-tok")
        await _create_balance(db_session, user.id, balance=5)
        ride = await _create_ride(
            db_session,
            user.id,
            last_reported_present=False,
            credits_charged=3,
        )

        with patch(
            "app.tasks.ride_verification.send_ride_credit_refunded",
            new_callable=AsyncMock,
        ) as mock_fcm:
            result = await process_user_verifications(user.id, db_session, fake_redis)

        # Ride status
        ride = await _reload_ride(db_session, ride.id)
        assert ride.verification_status == "CANCELLED"
        assert ride.verified_at is not None
        assert ride.credits_refunded == 3

        # Balance refunded
        balance = await _reload_balance(db_session, user.id)
        assert balance == 8  # 5 + 3

        # CreditTransaction RIDE_REFUND created
        tx_result = await db_session.execute(
            select(CreditTransaction).where(
                CreditTransaction.user_id == user.id,
                CreditTransaction.type == TransactionType.RIDE_REFUND,
            )
        )
        txn = tx_result.scalar_one()
        assert txn.amount == 3
        assert txn.balance_after == 8
        assert txn.reference_id == ride.id

        # Return value
        assert len(result) == 1
        assert result[0]["ride_id"] == ride.id
        assert result[0]["credits_refunded"] == 3
        assert result[0]["new_balance"] == 8

        # FCM push sent
        mock_fcm.assert_called_once_with(db_session, user.id, ride.id, 3, 8)

        # Redis cache updated
        cached = fake_redis._store.get(f"user_balance:{user.id}")
        assert cached == "8"

    @pytest.mark.asyncio
    async def test_fcm_failure_does_not_crash(self, db_session, fake_redis):
        """FCM send failure is logged but does not prevent processing."""
        user = await _create_user(db_session, fcm_token="tok")
        await _create_balance(db_session, user.id, balance=5)
        await _create_ride(
            db_session,
            user.id,
            last_reported_present=False,
            credits_charged=2,
        )

        with patch(
            "app.tasks.ride_verification.send_ride_credit_refunded",
            new_callable=AsyncMock,
            side_effect=RuntimeError("FCM down"),
        ):
            # Should NOT raise
            result = await process_user_verifications(user.id, db_session, fake_redis)

        # Processing still succeeded
        assert len(result) == 1
        balance = await _reload_balance(db_session, user.id)
        assert balance == 7  # 5 + 2

    @pytest.mark.asyncio
    async def test_double_processing_protection(self, db_session, fake_redis):
        """Second call finds no PENDING rides → empty result."""
        user = await _create_user(db_session)
        await _create_balance(db_session, user.id, balance=10)
        ride = await _create_ride(
            db_session,
            user.id,
            last_reported_present=True,
            credits_charged=2,
        )

        with patch(
            "app.tasks.ride_verification.send_ride_credit_refunded",
            new_callable=AsyncMock,
        ):
            result1 = await process_user_verifications(user.id, db_session, fake_redis)
            result2 = await process_user_verifications(user.id, db_session, fake_redis)

        ride = await _reload_ride(db_session, ride.id)
        assert ride.verification_status == "CONFIRMED"
        assert result1 == []
        assert result2 == []

    @pytest.mark.asyncio
    async def test_no_expired_rides_graceful(self, db_session, fake_redis):
        """No expired rides → empty result, no errors."""
        user = await _create_user(db_session)
        await _create_balance(db_session, user.id, balance=10)

        with patch(
            "app.tasks.ride_verification.send_ride_credit_refunded",
            new_callable=AsyncMock,
        ) as mock_fcm:
            result = await process_user_verifications(user.id, db_session, fake_redis)

        assert result == []
        mock_fcm.assert_not_called()

    @pytest.mark.asyncio
    async def test_multiple_rides_mixed_statuses(self, db_session, fake_redis):
        """Mix of present=True and present=False rides processed correctly."""
        user = await _create_user(db_session, fcm_token="tok")
        await _create_balance(db_session, user.id, balance=10)

        ride_confirmed = await _create_ride(
            db_session,
            user.id,
            last_reported_present=True,
            credits_charged=2,
        )
        ride_cancelled = await _create_ride(
            db_session,
            user.id,
            last_reported_present=False,
            credits_charged=3,
        )

        with patch(
            "app.tasks.ride_verification.send_ride_credit_refunded",
            new_callable=AsyncMock,
        ) as mock_fcm:
            result = await process_user_verifications(user.id, db_session, fake_redis)

        rc = await _reload_ride(db_session, ride_confirmed.id)
        assert rc.verification_status == "CONFIRMED"

        rx = await _reload_ride(db_session, ride_cancelled.id)
        assert rx.verification_status == "CANCELLED"
        assert rx.credits_refunded == 3

        assert len(result) == 1
        assert result[0]["ride_id"] == ride_cancelled.id
        mock_fcm.assert_called_once()

        balance = await _reload_balance(db_session, user.id)
        assert balance == 13  # 10 + 3


# ---------------------------------------------------------------------------
# run_verification_fallback — unit tests (mocked loop)
# ---------------------------------------------------------------------------


class TestRunVerificationFallback:
    @pytest.mark.asyncio
    async def test_loop_processes_users(self):
        """Loop finds users and calls process_user_verifications for each."""
        user_id = uuid.uuid4()
        mock_db = AsyncMock()

        @asynccontextmanager
        async def mock_session():
            yield mock_db

        call_count = 0

        async def stop_after_one(_seconds):
            nonlocal call_count
            call_count += 1
            if call_count >= 2:  # initial delay + first interval
                raise asyncio.CancelledError()

        with (
            patch(
                "app.tasks.ride_verification.AsyncSessionLocal",
                side_effect=mock_session,
            ),
            patch(
                "app.tasks.ride_verification.get_users_with_expired_rides",
                new_callable=AsyncMock,
                return_value=[user_id],
            ) as mock_get_users,
            patch(
                "app.tasks.ride_verification.process_user_verifications",
                new_callable=AsyncMock,
                return_value=[],
            ) as mock_process,
            patch(
                "app.tasks.ride_verification.asyncio.sleep",
                new_callable=AsyncMock,
                side_effect=stop_after_one,
            ),
            patch(
                "app.tasks.ride_verification.redis_client",
                new=AsyncMock(),
            ) as mock_redis,
        ):
            with pytest.raises(asyncio.CancelledError):
                await run_verification_fallback()

            mock_get_users.assert_called_once_with(mock_db)
            mock_process.assert_called_once_with(user_id, mock_db, mock_redis)

    @pytest.mark.asyncio
    async def test_loop_no_expired_rides(self):
        """No expired rides → no processing, no errors."""
        mock_db = AsyncMock()

        @asynccontextmanager
        async def mock_session():
            yield mock_db

        call_count = 0

        async def stop_after_one(_seconds):
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                raise asyncio.CancelledError()

        with (
            patch(
                "app.tasks.ride_verification.AsyncSessionLocal",
                side_effect=mock_session,
            ),
            patch(
                "app.tasks.ride_verification.get_users_with_expired_rides",
                new_callable=AsyncMock,
                return_value=[],
            ),
            patch(
                "app.tasks.ride_verification.process_user_verifications",
                new_callable=AsyncMock,
            ) as mock_process,
            patch(
                "app.tasks.ride_verification.asyncio.sleep",
                new_callable=AsyncMock,
                side_effect=stop_after_one,
            ),
        ):
            with pytest.raises(asyncio.CancelledError):
                await run_verification_fallback()

            mock_process.assert_not_called()

    @pytest.mark.asyncio
    async def test_loop_per_user_error_isolation(self):
        """Error processing one user does not prevent processing the next."""
        user_a = uuid.uuid4()
        user_b = uuid.uuid4()
        mock_db = AsyncMock()

        @asynccontextmanager
        async def mock_session():
            yield mock_db

        call_count = 0

        async def stop_after_one(_seconds):
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                raise asyncio.CancelledError()

        process_calls = []

        async def mock_process(uid, db, redis):
            process_calls.append(uid)
            if uid == user_a:
                raise RuntimeError("DB error for user A")
            return []

        with (
            patch(
                "app.tasks.ride_verification.AsyncSessionLocal",
                side_effect=mock_session,
            ),
            patch(
                "app.tasks.ride_verification.get_users_with_expired_rides",
                new_callable=AsyncMock,
                return_value=[user_a, user_b],
            ),
            patch(
                "app.tasks.ride_verification.process_user_verifications",
                side_effect=mock_process,
            ),
            patch(
                "app.tasks.ride_verification.asyncio.sleep",
                new_callable=AsyncMock,
                side_effect=stop_after_one,
            ),
            patch(
                "app.tasks.ride_verification.redis_client",
                new=AsyncMock(),
            ),
            pytest.raises(asyncio.CancelledError),
        ):
            await run_verification_fallback()

        # Both users were attempted despite user_a's error.
        assert process_calls == [user_a, user_b]

    @pytest.mark.asyncio
    async def test_loop_handles_session_error(self):
        """DB session error in user lookup does not crash the loop."""
        call_count = 0

        @asynccontextmanager
        async def mock_session():
            raise RuntimeError("DB connection failed")
            yield  # pragma: no cover

        async def count_and_stop(_seconds):
            nonlocal call_count
            call_count += 1
            if call_count >= 3:
                raise asyncio.CancelledError()

        with (
            patch(
                "app.tasks.ride_verification.AsyncSessionLocal",
                side_effect=mock_session,
            ),
            patch(
                "app.tasks.ride_verification.asyncio.sleep",
                new_callable=AsyncMock,
                side_effect=count_and_stop,
            ),
            pytest.raises(asyncio.CancelledError),
        ):
            await run_verification_fallback()

        # Survived initial delay + 2 error iterations before CancelledError.
        assert call_count == 3

    @pytest.mark.asyncio
    async def test_loop_uses_correct_interval(self):
        """Sleep interval matches VERIFICATION_INTERVAL_SECONDS (300s)."""
        mock_db = AsyncMock()

        @asynccontextmanager
        async def mock_session():
            yield mock_db

        sleep_values = []

        async def capture_sleep(seconds):
            sleep_values.append(seconds)
            if len(sleep_values) >= 2:
                raise asyncio.CancelledError()

        with (
            patch(
                "app.tasks.ride_verification.AsyncSessionLocal",
                side_effect=mock_session,
            ),
            patch(
                "app.tasks.ride_verification.get_users_with_expired_rides",
                new_callable=AsyncMock,
                return_value=[],
            ),
            patch(
                "app.tasks.ride_verification.asyncio.sleep",
                new_callable=AsyncMock,
                side_effect=capture_sleep,
            ),
            pytest.raises(asyncio.CancelledError),
        ):
            await run_verification_fallback()

        # First sleep = initial delay (20s), second = interval (300s)
        assert sleep_values[0] == 20
        assert sleep_values[1] == 300
