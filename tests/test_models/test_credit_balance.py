import uuid

import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from app.models.credit_balance import CreditBalance
from app.models.user import User


def _make_user(email: str = "test@example.com") -> User:
    return User(email=email, password_hash="hashed")


def _make_balance(user_id: uuid.UUID, **overrides) -> CreditBalance:
    defaults = {"user_id": user_id}
    defaults.update(overrides)
    return CreditBalance(**defaults)


# --- Test strategy item 1: create CreditBalance with valid user_id → success ---


async def test_create_credit_balance_success(db_session):
    """CreditBalance is created successfully with a valid user_id."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    balance = _make_balance(user.id, balance=10)
    db_session.add(balance)
    await db_session.flush()

    result = await db_session.execute(
        select(CreditBalance).where(CreditBalance.user_id == user.id)
    )
    loaded = result.scalar_one()
    assert loaded.balance == 10
    assert loaded.user_id == user.id
    assert loaded.id is not None
    assert loaded.updated_at is not None


# --- Test strategy item 2: CHECK constraint — balance < 0 → IntegrityError ---


async def test_check_constraint_negative_balance(db_session):
    """Setting balance below 0 raises IntegrityError (CHECK constraint)."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    balance = _make_balance(user.id, balance=-1)
    db_session.add(balance)
    with pytest.raises(IntegrityError):
        await db_session.flush()


# --- Test strategy item 3: UNIQUE constraint on user_id — duplicate → IntegrityError ---


async def test_unique_constraint_on_user_id(db_session):
    """Second CreditBalance for the same user_id raises IntegrityError."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    balance1 = _make_balance(user.id, balance=5)
    db_session.add(balance1)
    await db_session.flush()

    balance2 = _make_balance(user.id, balance=10)
    db_session.add(balance2)
    with pytest.raises(IntegrityError):
        await db_session.flush()


# --- Test strategy item 4: CASCADE delete — deleting User removes CreditBalance ---


async def test_cascade_delete_on_user_removal(db_session):
    """Deleting a User automatically deletes associated CreditBalance."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    balance = _make_balance(user.id, balance=10)
    db_session.add(balance)
    await db_session.flush()

    balance_id = balance.id

    await db_session.delete(user)
    await db_session.flush()

    result = await db_session.execute(select(CreditBalance).where(CreditBalance.id == balance_id))
    assert result.scalar_one_or_none() is None


# --- Test strategy item 5: one-to-one relationship — user.credit_balance returns single record ---


async def test_one_to_one_relationship(db_session):
    """user.credit_balance returns the single CreditBalance record."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    balance = _make_balance(user.id, balance=42)
    db_session.add(balance)
    await db_session.flush()

    # Refresh user to load relationship
    await db_session.refresh(user, ["credit_balance"])
    assert user.credit_balance is not None
    assert user.credit_balance.balance == 42
    assert user.credit_balance.user_id == user.id


# --- Additional: default balance is 0 ---


async def test_default_balance_is_zero(db_session):
    """balance defaults to 0 when not specified."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    balance = _make_balance(user.id)
    db_session.add(balance)
    await db_session.flush()

    assert balance.balance == 0


# --- Additional: updated_at is set on insert ---


async def test_updated_at_set_on_insert(db_session):
    """updated_at receives a server-generated timestamp on insert."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    balance = _make_balance(user.id)
    db_session.add(balance)
    await db_session.flush()

    result = await db_session.execute(
        select(CreditBalance).where(CreditBalance.user_id == user.id)
    )
    loaded = result.scalar_one()
    assert loaded.updated_at is not None
