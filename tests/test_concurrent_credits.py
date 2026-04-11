"""Tests for concurrent credit operations (P0-2).

Validates that charge_credits and refund_credits behave correctly under
concurrent access using SELECT FOR UPDATE NOWAIT row-level locking.
"""

import uuid
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.exc import OperationalError

from app.models.credit_balance import CreditBalance
from app.models.credit_transaction import CreditTransaction
from app.services.credit_service import (
    charge_credits,
    refund_credits,
)


@pytest.fixture
def mock_redis():
    """Minimal Redis mock for credit tests."""
    redis = AsyncMock()
    redis.get = AsyncMock(return_value=None)
    redis.setex = AsyncMock()
    redis.delete = AsyncMock()
    return redis


class TestChargeCreditsBasic:
    """Basic charge_credits correctness."""

    @pytest.mark.asyncio
    async def test_charge_deducts_correct_amount(self, db_session, mock_redis):
        """Charging deducts the requested amount and returns correct values."""
        from app.models.user import User

        user = User(email="charge@test.com", password_hash="hash")
        db_session.add(user)
        await db_session.flush()

        balance = CreditBalance(user_id=user.id, balance=100)
        db_session.add(balance)
        await db_session.commit()

        ref_id = uuid.uuid4()
        charged, new_balance = await charge_credits(user.id, 30, ref_id, db_session, mock_redis)
        await db_session.commit()

        assert charged == 30
        assert new_balance == 70

    @pytest.mark.asyncio
    async def test_charge_partial_when_insufficient(self, db_session, mock_redis):
        """Charges only available balance when amount exceeds balance."""
        from app.models.user import User

        user = User(email="partial@test.com", password_hash="hash")
        db_session.add(user)
        await db_session.flush()

        balance = CreditBalance(user_id=user.id, balance=20)
        db_session.add(balance)
        await db_session.commit()

        ref_id = uuid.uuid4()
        charged, new_balance = await charge_credits(user.id, 50, ref_id, db_session, mock_redis)
        await db_session.commit()

        assert charged == 20
        assert new_balance == 0

    @pytest.mark.asyncio
    async def test_charge_zero_balance_returns_zero(self, db_session, mock_redis):
        """No transaction created when balance is zero."""
        from app.models.user import User

        user = User(email="zero@test.com", password_hash="hash")
        db_session.add(user)
        await db_session.flush()

        balance = CreditBalance(user_id=user.id, balance=0)
        db_session.add(balance)
        await db_session.commit()

        ref_id = uuid.uuid4()
        charged, new_balance = await charge_credits(user.id, 10, ref_id, db_session, mock_redis)

        assert charged == 0
        assert new_balance == 0


class TestRefundCreditsBasic:
    """Basic refund_credits correctness."""

    @pytest.mark.asyncio
    async def test_refund_adds_credits(self, db_session, mock_redis):
        """Refund adds the specified amount to the balance."""
        from app.models.user import User

        user = User(email="refund@test.com", password_hash="hash")
        db_session.add(user)
        await db_session.flush()

        balance = CreditBalance(user_id=user.id, balance=50)
        db_session.add(balance)
        await db_session.commit()

        ref_id = uuid.uuid4()
        new_balance = await refund_credits(user.id, 25, ref_id, db_session, mock_redis)

        assert new_balance == 75

    @pytest.mark.asyncio
    async def test_refund_rejects_zero_amount(self, db_session, mock_redis):
        """ValueError raised for non-positive refund amount."""
        user_id = uuid.uuid4()
        ref_id = uuid.uuid4()
        with pytest.raises(ValueError, match="positive"):
            await refund_credits(user_id, 0, ref_id, db_session, mock_redis)

    @pytest.mark.asyncio
    async def test_refund_requires_reference_id(self, db_session, mock_redis):
        """ValueError raised when reference_id is None."""
        user_id = uuid.uuid4()
        with pytest.raises(ValueError, match="reference_id"):
            await refund_credits(user_id, 10, None, db_session, mock_redis)


class TestChargeRefundSequence:
    """Tests for charge followed by refund — net balance correctness."""

    @pytest.mark.asyncio
    async def test_charge_then_refund_restores_balance(self, db_session, mock_redis):
        """Charge followed by refund of same amount restores original balance."""
        from app.models.user import User

        user = User(email="seq@test.com", password_hash="hash")
        db_session.add(user)
        await db_session.flush()

        balance = CreditBalance(user_id=user.id, balance=100)
        db_session.add(balance)
        await db_session.commit()

        ref_id = uuid.uuid4()
        _charged, after_charge = await charge_credits(user.id, 40, ref_id, db_session, mock_redis)
        await db_session.commit()
        assert after_charge == 60

        after_refund = await refund_credits(user.id, 40, ref_id, db_session, mock_redis)
        assert after_refund == 100

    @pytest.mark.asyncio
    async def test_multiple_charges_create_transactions(self, db_session, mock_redis):
        """Multiple charges create separate transaction records."""
        from app.models.user import User

        user = User(email="multi@test.com", password_hash="hash")
        db_session.add(user)
        await db_session.flush()

        balance = CreditBalance(user_id=user.id, balance=100)
        db_session.add(balance)
        await db_session.commit()

        for _i in range(3):
            ref_id = uuid.uuid4()
            await charge_credits(user.id, 10, ref_id, db_session, mock_redis)
            await db_session.commit()

        result = await db_session.execute(
            select(CreditTransaction).where(CreditTransaction.user_id == user.id)
        )
        transactions = result.scalars().all()
        assert len(transactions) == 3

        # Final balance should be 100 - 30 = 70
        result = await db_session.execute(
            select(CreditBalance).where(CreditBalance.user_id == user.id)
        )
        final = result.scalar_one()
        assert final.balance == 70


class TestConcurrentLocking:
    """Tests for SELECT FOR UPDATE NOWAIT locking behavior."""

    @pytest.mark.asyncio
    async def test_charge_raises_503_on_lock_conflict(self, db_session, mock_redis):
        """charge_credits raises 503 when row lock cannot be obtained."""
        from app.models.user import User

        user = User(email="lock@test.com", password_hash="hash")
        db_session.add(user)
        await db_session.flush()

        balance = CreditBalance(user_id=user.id, balance=100)
        db_session.add(balance)
        await db_session.commit()

        ref_id = uuid.uuid4()

        # Simulate lock conflict by patching db.execute to raise OperationalError
        original_execute = db_session.execute

        async def mock_execute(stmt, *args, **kwargs):
            compiled = str(stmt.compile()) if hasattr(stmt, "compile") else str(stmt)
            if "FOR UPDATE" in compiled:
                raise OperationalError(
                    "could not obtain lock on row",
                    params=None,
                    orig=Exception("could not obtain lock on row in relation"),
                )
            return await original_execute(stmt, *args, **kwargs)

        with patch.object(db_session, "execute", side_effect=mock_execute):
            with pytest.raises(HTTPException) as exc_info:
                await charge_credits(user.id, 10, ref_id, db_session, mock_redis)
            assert exc_info.value.status_code == 503
            assert "locked" in exc_info.value.detail.lower()
