"""Tests for purchase_recovery background task (task 8.2).

Test strategy:
1. CONSUMED order older than 2 min → credits applied, status VERIFIED
2. CONSUMED order younger than 2 min → not touched (still in active request)
3. Order older than 24 hours → PURCHASE_STUCK warning, status unchanged
4. Idempotency: already VERIFIED order → skip (affected_rows=0)
5. CreditTransaction created with correct type PURCHASE
6. Double-processing protection via affected_rows check
7. affected_rows=0 at concurrent processing → log success, don't apply credits
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
from app.models.purchase_order import PurchaseOrder, PurchaseStatus
from app.models.user import User
from app.tasks.purchase_recovery import (
    get_recoverable_order_ids,
    get_stuck_order_ids,
    recover_order,
    run_purchase_recovery,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _create_user(db: AsyncSession) -> User:
    user = User(
        email=f"recovery-{uuid.uuid4().hex[:8]}@example.com",
        password_hash="hashed",
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


async def _create_order(
    db: AsyncSession,
    user_id: uuid.UUID,
    *,
    status: str = PurchaseStatus.CONSUMED.value,
    credits_amount: int = 25,
    created_at: datetime | None = None,
) -> PurchaseOrder:
    order = PurchaseOrder(
        user_id=user_id,
        product_id="credits_25",
        purchase_token=f"token-{uuid.uuid4().hex}",
        credits_amount=credits_amount,
        status=status,
    )
    db.add(order)
    await db.flush()

    if created_at is not None:
        # Override server_default created_at via raw UPDATE
        from sqlalchemy import update

        await db.execute(
            update(PurchaseOrder).where(PurchaseOrder.id == order.id).values(created_at=created_at)
        )
        await db.flush()
        await db.refresh(order)

    return order


async def _reload_order(db: AsyncSession, order_id: uuid.UUID) -> PurchaseOrder:
    result = await db.execute(
        select(PurchaseOrder)
        .where(PurchaseOrder.id == order_id)
        .execution_options(populate_existing=True)
    )
    return result.scalar_one()


async def _reload_balance(db: AsyncSession, user_id: uuid.UUID) -> int:
    result = await db.execute(
        select(CreditBalance.balance).where(CreditBalance.user_id == user_id)
    )
    return result.scalar_one()


# ---------------------------------------------------------------------------
# get_recoverable_order_ids — integration tests
# ---------------------------------------------------------------------------


class TestGetRecoverableOrderIds:
    @pytest.mark.asyncio
    async def test_returns_consumed_order_older_than_2_min(self, db_session):
        """CONSUMED order created 10 minutes ago → eligible for recovery."""
        user = await _create_user(db_session)
        order = await _create_order(
            db_session,
            user.id,
            created_at=datetime.now(UTC) - timedelta(minutes=10),
        )

        ids = await get_recoverable_order_ids(db_session)
        assert order.id in ids

    @pytest.mark.asyncio
    async def test_excludes_order_younger_than_2_min(self, db_session):
        """CONSUMED order created 1 minute ago → skip (still in active request)."""
        user = await _create_user(db_session)
        await _create_order(
            db_session,
            user.id,
            created_at=datetime.now(UTC) - timedelta(minutes=1),
        )

        ids = await get_recoverable_order_ids(db_session)
        assert ids == []

    @pytest.mark.asyncio
    async def test_excludes_order_older_than_24_hours(self, db_session):
        """CONSUMED order created 25 hours ago → excluded (anomalous)."""
        user = await _create_user(db_session)
        await _create_order(
            db_session,
            user.id,
            created_at=datetime.now(UTC) - timedelta(hours=25),
        )

        ids = await get_recoverable_order_ids(db_session)
        assert ids == []

    @pytest.mark.asyncio
    async def test_excludes_non_consumed_statuses(self, db_session):
        """PENDING, VERIFIED, FAILED orders are NOT included."""
        user = await _create_user(db_session)
        age = datetime.now(UTC) - timedelta(minutes=10)

        for status in (
            PurchaseStatus.PENDING.value,
            PurchaseStatus.VERIFIED.value,
            PurchaseStatus.FAILED.value,
        ):
            await _create_order(db_session, user.id, status=status, created_at=age)

        ids = await get_recoverable_order_ids(db_session)
        assert ids == []

    @pytest.mark.asyncio
    async def test_empty_when_no_orders(self, db_session):
        """No orders at all → empty list."""
        ids = await get_recoverable_order_ids(db_session)
        assert ids == []


# ---------------------------------------------------------------------------
# get_stuck_order_ids — integration tests
# ---------------------------------------------------------------------------


class TestGetStuckOrderIds:
    @pytest.mark.asyncio
    async def test_returns_consumed_order_older_than_24_hours(self, db_session):
        """CONSUMED order created 25 hours ago → stuck."""
        user = await _create_user(db_session)
        order = await _create_order(
            db_session,
            user.id,
            created_at=datetime.now(UTC) - timedelta(hours=25),
        )

        ids = await get_stuck_order_ids(db_session)
        assert order.id in ids

    @pytest.mark.asyncio
    async def test_excludes_recent_consumed_orders(self, db_session):
        """CONSUMED order created 10 minutes ago → not stuck."""
        user = await _create_user(db_session)
        await _create_order(
            db_session,
            user.id,
            created_at=datetime.now(UTC) - timedelta(minutes=10),
        )

        ids = await get_stuck_order_ids(db_session)
        assert ids == []

    @pytest.mark.asyncio
    async def test_excludes_non_consumed_statuses(self, db_session):
        """VERIFIED order older than 24h → not stuck (already processed)."""
        user = await _create_user(db_session)
        await _create_order(
            db_session,
            user.id,
            status=PurchaseStatus.VERIFIED.value,
            created_at=datetime.now(UTC) - timedelta(hours=25),
        )

        ids = await get_stuck_order_ids(db_session)
        assert ids == []


# ---------------------------------------------------------------------------
# recover_order — integration tests
# ---------------------------------------------------------------------------


class TestRecoverOrder:
    @pytest.mark.asyncio
    async def test_consumed_order_recovered_credits_applied(self, db_session, fake_redis):
        """CONSUMED order → VERIFIED, credits added to balance."""
        user = await _create_user(db_session)
        await _create_balance(db_session, user.id, balance=10)
        order = await _create_order(
            db_session,
            user.id,
            credits_amount=25,
            created_at=datetime.now(UTC) - timedelta(minutes=10),
        )

        recovered = await recover_order(order.id, db_session, fake_redis)

        assert recovered is True

        order = await _reload_order(db_session, order.id)
        assert order.status == PurchaseStatus.VERIFIED.value
        assert order.verified_at is not None

        balance = await _reload_balance(db_session, user.id)
        assert balance == 35  # 10 + 25

    @pytest.mark.asyncio
    async def test_credit_transaction_created_with_correct_type(self, db_session, fake_redis):
        """CreditTransaction with type=PURCHASE and correct reference_id."""
        user = await _create_user(db_session)
        await _create_balance(db_session, user.id, balance=5)
        order = await _create_order(
            db_session,
            user.id,
            credits_amount=50,
            created_at=datetime.now(UTC) - timedelta(minutes=10),
        )

        await recover_order(order.id, db_session, fake_redis)

        tx_result = await db_session.execute(
            select(CreditTransaction).where(
                CreditTransaction.user_id == user.id,
                CreditTransaction.type == TransactionType.PURCHASE,
            )
        )
        txn = tx_result.scalar_one()
        assert txn.amount == 50
        assert txn.balance_after == 55  # 5 + 50
        assert txn.reference_id == order.id

    @pytest.mark.asyncio
    async def test_redis_cache_updated_after_recovery(self, db_session, fake_redis):
        """Redis balance cache is updated after successful recovery."""
        user = await _create_user(db_session)
        await _create_balance(db_session, user.id, balance=10)
        order = await _create_order(
            db_session,
            user.id,
            credits_amount=25,
            created_at=datetime.now(UTC) - timedelta(minutes=10),
        )

        await recover_order(order.id, db_session, fake_redis)

        cached = fake_redis._store.get(f"user_balance:{user.id}")
        assert cached == "35"

    @pytest.mark.asyncio
    async def test_already_verified_order_skipped(self, db_session, fake_redis):
        """VERIFIED order → rowcount=0, returns False, no credit changes."""
        user = await _create_user(db_session)
        await _create_balance(db_session, user.id, balance=10)
        order = await _create_order(
            db_session,
            user.id,
            status=PurchaseStatus.VERIFIED.value,
            credits_amount=25,
            created_at=datetime.now(UTC) - timedelta(minutes=10),
        )

        recovered = await recover_order(order.id, db_session, fake_redis)

        assert recovered is False
        balance = await _reload_balance(db_session, user.id)
        assert balance == 10  # unchanged

    @pytest.mark.asyncio
    async def test_double_processing_protection(self, db_session, fake_redis):
        """Second recover_order call on same order → False (already VERIFIED)."""
        user = await _create_user(db_session)
        await _create_balance(db_session, user.id, balance=10)
        order = await _create_order(
            db_session,
            user.id,
            credits_amount=25,
            created_at=datetime.now(UTC) - timedelta(minutes=10),
        )

        result1 = await recover_order(order.id, db_session, fake_redis)
        result2 = await recover_order(order.id, db_session, fake_redis)

        assert result1 is True
        assert result2 is False

        # Credits applied only once
        balance = await _reload_balance(db_session, user.id)
        assert balance == 35  # 10 + 25, not 10 + 50

    @pytest.mark.asyncio
    async def test_nonexistent_order_returns_false(self, db_session, fake_redis):
        """Order ID that doesn't exist → returns False gracefully."""
        recovered = await recover_order(uuid.uuid4(), db_session, fake_redis)
        assert recovered is False


# ---------------------------------------------------------------------------
# run_purchase_recovery — unit tests (mocked loop)
# ---------------------------------------------------------------------------


class TestRunPurchaseRecovery:
    @pytest.mark.asyncio
    async def test_loop_recovers_orders(self):
        """Loop finds CONSUMED orders and calls recover_order for each."""
        order_id = uuid.uuid4()
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
                "app.tasks.purchase_recovery.AsyncSessionLocal",
                side_effect=mock_session,
            ),
            patch(
                "app.tasks.purchase_recovery.get_recoverable_order_ids",
                new_callable=AsyncMock,
                return_value=[order_id],
            ) as mock_get_orders,
            patch(
                "app.tasks.purchase_recovery.get_stuck_order_ids",
                new_callable=AsyncMock,
                return_value=[],
            ),
            patch(
                "app.tasks.purchase_recovery.recover_order",
                new_callable=AsyncMock,
                return_value=True,
            ) as mock_recover,
            patch(
                "app.tasks.purchase_recovery.asyncio.sleep",
                new_callable=AsyncMock,
                side_effect=stop_after_one,
            ),
            patch(
                "app.tasks.purchase_recovery.redis_client",
                new=AsyncMock(),
            ) as mock_redis,
        ):
            with pytest.raises(asyncio.CancelledError):
                await run_purchase_recovery()

            mock_get_orders.assert_called_once_with(mock_db)
            mock_recover.assert_called_once_with(order_id, mock_db, mock_redis)

    @pytest.mark.asyncio
    async def test_loop_no_orders(self):
        """No CONSUMED orders → no recovery calls, no errors."""
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
                "app.tasks.purchase_recovery.AsyncSessionLocal",
                side_effect=mock_session,
            ),
            patch(
                "app.tasks.purchase_recovery.get_recoverable_order_ids",
                new_callable=AsyncMock,
                return_value=[],
            ),
            patch(
                "app.tasks.purchase_recovery.get_stuck_order_ids",
                new_callable=AsyncMock,
                return_value=[],
            ),
            patch(
                "app.tasks.purchase_recovery.recover_order",
                new_callable=AsyncMock,
            ) as mock_recover,
            patch(
                "app.tasks.purchase_recovery.asyncio.sleep",
                new_callable=AsyncMock,
                side_effect=stop_after_one,
            ),
        ):
            with pytest.raises(asyncio.CancelledError):
                await run_purchase_recovery()

            mock_recover.assert_not_called()

    @pytest.mark.asyncio
    async def test_loop_per_order_error_isolation(self):
        """Error recovering one order does not prevent processing the next."""
        order_a = uuid.uuid4()
        order_b = uuid.uuid4()
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

        recover_calls = []

        async def mock_recover(oid, db, redis):
            recover_calls.append(oid)
            if oid == order_a:
                raise RuntimeError("DB error for order A")
            return True

        with (
            patch(
                "app.tasks.purchase_recovery.AsyncSessionLocal",
                side_effect=mock_session,
            ),
            patch(
                "app.tasks.purchase_recovery.get_recoverable_order_ids",
                new_callable=AsyncMock,
                return_value=[order_a, order_b],
            ),
            patch(
                "app.tasks.purchase_recovery.get_stuck_order_ids",
                new_callable=AsyncMock,
                return_value=[],
            ),
            patch(
                "app.tasks.purchase_recovery.recover_order",
                side_effect=mock_recover,
            ),
            patch(
                "app.tasks.purchase_recovery.asyncio.sleep",
                new_callable=AsyncMock,
                side_effect=stop_after_one,
            ),
            patch(
                "app.tasks.purchase_recovery.redis_client",
                new=AsyncMock(),
            ),
            pytest.raises(asyncio.CancelledError),
        ):
            await run_purchase_recovery()

        assert recover_calls == [order_a, order_b]

    @pytest.mark.asyncio
    async def test_loop_logs_stuck_orders(self):
        """Stuck orders (> 24h) are logged as PURCHASE_STUCK warning."""
        stuck_id = uuid.uuid4()
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
                "app.tasks.purchase_recovery.AsyncSessionLocal",
                side_effect=mock_session,
            ),
            patch(
                "app.tasks.purchase_recovery.get_recoverable_order_ids",
                new_callable=AsyncMock,
                return_value=[],
            ),
            patch(
                "app.tasks.purchase_recovery.get_stuck_order_ids",
                new_callable=AsyncMock,
                return_value=[stuck_id],
            ),
            patch(
                "app.tasks.purchase_recovery.asyncio.sleep",
                new_callable=AsyncMock,
                side_effect=stop_after_one,
            ),
            patch(
                "app.tasks.purchase_recovery.logger",
            ) as mock_logger,
        ):
            with pytest.raises(asyncio.CancelledError):
                await run_purchase_recovery()

            # Verify PURCHASE_STUCK was logged
            mock_logger.warning.assert_called()
            warning_args = mock_logger.warning.call_args
            assert "PURCHASE_STUCK" in warning_args[0][0]

    @pytest.mark.asyncio
    async def test_loop_handles_session_error(self):
        """DB session error does not crash the loop."""
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
                "app.tasks.purchase_recovery.AsyncSessionLocal",
                side_effect=mock_session,
            ),
            patch(
                "app.tasks.purchase_recovery.asyncio.sleep",
                new_callable=AsyncMock,
                side_effect=count_and_stop,
            ),
            pytest.raises(asyncio.CancelledError),
        ):
            await run_purchase_recovery()

        # Survived initial delay + 2 error iterations before CancelledError
        assert call_count == 3

    @pytest.mark.asyncio
    async def test_loop_uses_correct_intervals(self):
        """Initial delay = 40s, interval = 300s."""
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
                "app.tasks.purchase_recovery.AsyncSessionLocal",
                side_effect=mock_session,
            ),
            patch(
                "app.tasks.purchase_recovery.get_recoverable_order_ids",
                new_callable=AsyncMock,
                return_value=[],
            ),
            patch(
                "app.tasks.purchase_recovery.get_stuck_order_ids",
                new_callable=AsyncMock,
                return_value=[],
            ),
            patch(
                "app.tasks.purchase_recovery.asyncio.sleep",
                new_callable=AsyncMock,
                side_effect=capture_sleep,
            ),
            pytest.raises(asyncio.CancelledError),
        ):
            await run_purchase_recovery()

        assert sleep_values[0] == 40  # INITIAL_DELAY_SECONDS
        assert sleep_values[1] == 300  # RECOVERY_INTERVAL_SECONDS
