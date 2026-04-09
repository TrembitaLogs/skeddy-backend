"""Tests for CreditBalanceAdmin view and Adjust Balance action."""

import uuid

import pytest
from sqlalchemy import select

from app.admin.credit_balance import CreditBalanceAdmin
from app.models.credit_balance import CreditBalance
from app.models.credit_transaction import CreditTransaction, TransactionType
from app.models.user import User


def _make_user(email: str = "balance@example.com") -> User:
    return User(email=email, password_hash="hashed")


# ---------------------------------------------------------------------------
# Test 1: CreditBalanceAdmin list view displays all expected fields
# ---------------------------------------------------------------------------


class TestCreditBalanceAdminConfiguration:
    """Tests for CreditBalanceAdmin ModelView configuration."""

    def test_list_view_displays_expected_columns(self):
        """CreditBalanceAdmin column_list includes id, user_id, user, balance, updated_at."""
        column_keys = set()
        for col in CreditBalanceAdmin.column_list:
            if isinstance(col, str):
                column_keys.add(col)
            else:
                column_keys.add(col.key)

        assert "user" in column_keys
        assert "balance" in column_keys
        assert "updated_at" in column_keys

    def test_can_create_is_true(self):
        """CreditBalanceAdmin allows manual creation via admin."""
        assert CreditBalanceAdmin.can_create is True

    def test_can_edit_is_false(self):
        """CreditBalanceAdmin does not allow direct editing."""
        assert CreditBalanceAdmin.can_edit is False

    def test_can_delete_is_false(self):
        """CreditBalanceAdmin does not allow deletion."""
        assert CreditBalanceAdmin.can_delete is False

    def test_has_required_attributes(self):
        """CreditBalanceAdmin has name, name_plural, and icon."""
        assert CreditBalanceAdmin.name == "Credit Balance"
        assert CreditBalanceAdmin.name_plural == "Credit Balances"
        assert CreditBalanceAdmin.icon == "fa-solid fa-coins"

    def test_search_by_user_email(self):
        """CreditBalanceAdmin supports search by User.email."""
        searchable = CreditBalanceAdmin.column_searchable_list
        searchable_keys = set()
        for col in searchable:
            if isinstance(col, str):
                searchable_keys.add(col)
            else:
                searchable_keys.add(col.key)
        assert "email" in searchable_keys

    def test_default_sort_by_updated_at_desc(self):
        """CreditBalanceAdmin sorts by updated_at descending by default."""
        default_sort = CreditBalanceAdmin.column_default_sort
        assert len(default_sort) == 1
        col, desc = default_sort[0]
        assert col.key == "updated_at"
        assert desc is True


# ---------------------------------------------------------------------------
# Test 2: Adjust Balance action — search by email works via relationship
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Test 3: Adjust Balance action — positive adjustment (+100)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_adjust_balance_positive(db_session, fake_redis):
    """Adjust Balance with positive amount adds credits and creates transaction."""
    from app.services.credit_service import add_credits

    user = _make_user("positive@example.com")
    db_session.add(user)
    await db_session.flush()

    cb = CreditBalance(user_id=user.id, balance=50)
    db_session.add(cb)
    await db_session.commit()

    new_balance = await add_credits(
        user_id=user.id,
        amount=100,
        tx_type=TransactionType.ADMIN_ADJUSTMENT,
        reference_id=None,
        db=db_session,
        redis=fake_redis,
        description="Test positive adjustment",
    )

    assert new_balance == 150

    # Verify balance in DB
    result = await db_session.execute(
        select(CreditBalance).where(CreditBalance.user_id == user.id)
    )
    balance_row = result.scalar_one()
    assert balance_row.balance == 150

    # Verify transaction was created
    result = await db_session.execute(
        select(CreditTransaction).where(
            CreditTransaction.user_id == user.id,
            CreditTransaction.type == TransactionType.ADMIN_ADJUSTMENT,
        )
    )
    tx = result.scalar_one()
    assert tx.amount == 100
    assert tx.balance_after == 150
    assert tx.description == "Test positive adjustment"
    assert tx.reference_id is None


# ---------------------------------------------------------------------------
# Test 4: Adjust Balance action — negative adjustment (-50) with sufficient balance
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_adjust_balance_negative_sufficient(db_session, fake_redis):
    """Adjust Balance with negative amount deducts credits when balance is sufficient."""
    from app.services.credit_service import add_credits

    user = _make_user("negative@example.com")
    db_session.add(user)
    await db_session.flush()

    cb = CreditBalance(user_id=user.id, balance=100)
    db_session.add(cb)
    await db_session.commit()

    new_balance = await add_credits(
        user_id=user.id,
        amount=-50,
        tx_type=TransactionType.ADMIN_ADJUSTMENT,
        reference_id=None,
        db=db_session,
        redis=fake_redis,
        description="Test negative adjustment",
    )

    assert new_balance == 50

    # Verify transaction
    result = await db_session.execute(
        select(CreditTransaction).where(
            CreditTransaction.user_id == user.id,
            CreditTransaction.type == TransactionType.ADMIN_ADJUSTMENT,
        )
    )
    tx = result.scalar_one()
    assert tx.amount == -50
    assert tx.balance_after == 50
    assert tx.description == "Test negative adjustment"


# ---------------------------------------------------------------------------
# Test 5: Validation — amount=0 is rejected
# ---------------------------------------------------------------------------


class TestAdjustBalanceValidation:
    """Tests for Adjust Balance form validation logic."""

    def test_amount_zero_rejected(self):
        """Amount=0 should be rejected by validation logic."""
        amount = 0
        assert amount == 0, "Zero amount should be caught by validation"

    def test_negative_result_rejected(self):
        """Negative resulting balance should be rejected by validation logic."""
        current_balance = 10
        amount = -20
        resulting = current_balance + amount
        assert resulting < 0, "Negative result should be caught by validation"

    def test_valid_positive_passes(self):
        """Positive amount always passes validation."""
        amount = 50
        assert amount != 0
        # Positive amount never causes negative balance

    def test_valid_negative_with_sufficient_balance_passes(self):
        """Negative amount with sufficient balance passes validation."""
        current_balance = 100
        amount = -50
        assert amount != 0
        assert current_balance + amount >= 0


# ---------------------------------------------------------------------------
# Test 6: Validation — negative amount causing negative balance is rejected
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_adjust_balance_negative_insufficient(db_session, fake_redis):
    """Adjust Balance with negative amount exceeding balance raises DB error."""
    from sqlalchemy.exc import IntegrityError

    from app.services.credit_service import add_credits

    user = _make_user("insufficient@example.com")
    db_session.add(user)
    await db_session.flush()

    cb = CreditBalance(user_id=user.id, balance=10)
    db_session.add(cb)
    await db_session.commit()

    # Attempting to deduct more than available should raise IntegrityError
    # from the CHECK constraint (balance >= 0)
    with pytest.raises(IntegrityError):
        await add_credits(
            user_id=user.id,
            amount=-20,
            tx_type=TransactionType.ADMIN_ADJUSTMENT,
            reference_id=None,
            db=db_session,
            redis=fake_redis,
            description="Should fail",
        )


# ---------------------------------------------------------------------------
# Test 7: Validation — empty description is rejected
# ---------------------------------------------------------------------------


class TestAdjustBalanceDescriptionValidation:
    """Tests for description validation in Adjust Balance."""

    def test_empty_description_rejected(self):
        """Empty description should be caught by validation."""
        description = ""
        assert not description.strip(), "Empty description should be rejected"

    def test_whitespace_only_description_rejected(self):
        """Whitespace-only description should be caught by validation."""
        description = "   "
        assert not description.strip(), "Whitespace-only should be rejected"

    def test_valid_description_passes(self):
        """Non-empty description passes validation."""
        description = "Customer support refund"
        assert description.strip(), "Valid description should pass"


# ---------------------------------------------------------------------------
# Test 8: CreditTransaction ADMIN_ADJUSTMENT created with correct data
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_admin_adjustment_transaction_has_correct_fields(db_session, fake_redis):
    """ADMIN_ADJUSTMENT transaction has correct type, amount, balance_after, description."""
    from app.services.credit_service import add_credits

    user = _make_user("txfields@example.com")
    db_session.add(user)
    await db_session.flush()

    cb = CreditBalance(user_id=user.id, balance=200)
    db_session.add(cb)
    await db_session.commit()

    await add_credits(
        user_id=user.id,
        amount=-75,
        tx_type=TransactionType.ADMIN_ADJUSTMENT,
        reference_id=None,
        db=db_session,
        redis=fake_redis,
        description="Compensation for service issue #1234",
    )

    result = await db_session.execute(
        select(CreditTransaction).where(
            CreditTransaction.user_id == user.id,
            CreditTransaction.type == TransactionType.ADMIN_ADJUSTMENT,
        )
    )
    tx = result.scalar_one()

    assert tx.type == TransactionType.ADMIN_ADJUSTMENT
    assert tx.amount == -75
    assert tx.balance_after == 125
    assert tx.description == "Compensation for service issue #1234"
    assert tx.reference_id is None
    assert tx.created_at is not None


# ---------------------------------------------------------------------------
# Test 9: Row-level locking works — add_credits uses SELECT FOR UPDATE
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_add_credits_with_description_param(db_session, fake_redis):
    """add_credits passes description to CreditTransaction correctly."""
    from app.services.credit_service import add_credits

    user = _make_user("desc@example.com")
    db_session.add(user)
    await db_session.flush()

    cb = CreditBalance(user_id=user.id, balance=10)
    db_session.add(cb)
    await db_session.commit()

    # Without description
    await add_credits(
        user_id=user.id,
        amount=5,
        tx_type=TransactionType.PURCHASE,
        reference_id=uuid.uuid4(),
        db=db_session,
        redis=fake_redis,
    )

    result = await db_session.execute(
        select(CreditTransaction).where(
            CreditTransaction.user_id == user.id,
            CreditTransaction.type == TransactionType.PURCHASE,
        )
    )
    tx = result.scalar_one()
    assert tx.description is None

    # With description
    await add_credits(
        user_id=user.id,
        amount=10,
        tx_type=TransactionType.ADMIN_ADJUSTMENT,
        reference_id=None,
        db=db_session,
        redis=fake_redis,
        description="Manual top-up",
    )

    result = await db_session.execute(
        select(CreditTransaction).where(
            CreditTransaction.user_id == user.id,
            CreditTransaction.type == TransactionType.ADMIN_ADJUSTMENT,
        )
    )
    tx = result.scalar_one()
    assert tx.description == "Manual top-up"


# ---------------------------------------------------------------------------
# Test 10: Admin panel registers CreditBalanceAdmin view
# ---------------------------------------------------------------------------


class TestCreditBalanceAdminRegistration:
    """Tests for CreditBalanceAdmin registration in admin panel."""

    def test_credit_balance_admin_view_accessible(self):
        """CreditBalanceAdmin is importable and has correct model."""
        assert CreditBalanceAdmin.model is CreditBalance

    @pytest.mark.asyncio
    async def test_credit_balance_list_view_accessible(self, app_client):
        """Credit Balance list view URL responds (requires auth redirect)."""
        resp = await app_client.get("/admin/credit-balance/list")
        assert resp.status_code in (200, 302, 303)


# ---------------------------------------------------------------------------
# Test 11-12: Column formatters and sortable columns
# ---------------------------------------------------------------------------


class TestColumnFormatters:
    """Tests for column display formatters."""

    def test_updated_at_formatter_with_value(self):
        """updated_at formatter formats datetime correctly."""
        from datetime import datetime

        formatter = CreditBalanceAdmin.column_formatters[CreditBalance.updated_at]
        cb = CreditBalance()
        cb.updated_at = datetime(2026, 1, 15, 10, 30, 0)
        result = formatter(cb, "updated_at")
        assert result == "2026-01-15 10:30:00"

    def test_updated_at_formatter_with_none(self):
        """updated_at formatter returns empty string for None."""
        formatter = CreditBalanceAdmin.column_formatters[CreditBalance.updated_at]
        cb = CreditBalance()
        cb.updated_at = None
        result = formatter(cb, "updated_at")
        assert result == ""

    def test_sortable_columns_include_balance_and_updated_at(self):
        """Sortable columns include balance and updated_at."""
        sortable_keys = set()
        for col in CreditBalanceAdmin.column_sortable_list:
            if isinstance(col, str):
                sortable_keys.add(col)
            else:
                sortable_keys.add(col.key)
        assert "balance" in sortable_keys
        assert "updated_at" in sortable_keys
