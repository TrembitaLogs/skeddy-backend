import uuid

import pytest
from sqlalchemy import select

from app.models.credit_transaction import CreditTransaction, TransactionType
from app.models.ride import Ride
from app.models.user import User


def _make_user(email: str = "test@example.com") -> User:
    return User(email=email, password_hash="hashed")


def _make_transaction(user_id: uuid.UUID, **overrides) -> CreditTransaction:
    defaults = {
        "user_id": user_id,
        "type": TransactionType.REGISTRATION_BONUS.value,
        "amount": 10,
        "balance_after": 10,
    }
    defaults.update(overrides)
    return CreditTransaction(**defaults)


# --- Test strategy item 1: create transaction of each type → success ---


@pytest.mark.parametrize("tx_type", list(TransactionType))
async def test_create_transaction_each_type(db_session, tx_type):
    """CreditTransaction is created successfully for each TransactionType."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    tx = _make_transaction(user.id, type=tx_type.value, amount=5, balance_after=15)
    db_session.add(tx)
    await db_session.flush()

    result = await db_session.execute(
        select(CreditTransaction).where(CreditTransaction.id == tx.id)
    )
    loaded = result.scalar_one()
    assert loaded.type == tx_type.value
    assert loaded.amount == 5
    assert loaded.balance_after == 15
    assert loaded.user_id == user.id
    assert loaded.id is not None
    assert loaded.created_at is not None


# --- Test strategy item 2: polymorphic reference — PURCHASE with reference_id ---


async def test_polymorphic_reference_purchase(db_session):
    """PURCHASE transaction stores reference_id as a generic UUID (no FK)."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    # reference_id points to a purchase_order — model doesn't exist yet,
    # but reference_id has no FK constraint so any UUID is accepted.
    fake_purchase_order_id = uuid.uuid4()
    tx = _make_transaction(
        user.id,
        type=TransactionType.PURCHASE.value,
        amount=50,
        balance_after=60,
        reference_id=fake_purchase_order_id,
    )
    db_session.add(tx)
    await db_session.flush()

    result = await db_session.execute(
        select(CreditTransaction).where(CreditTransaction.id == tx.id)
    )
    loaded = result.scalar_one()
    assert loaded.reference_id == fake_purchase_order_id
    assert loaded.type == TransactionType.PURCHASE.value


# --- Test strategy item 3: polymorphic reference — RIDE_CHARGE with ride.id ---


async def test_polymorphic_reference_ride_charge(db_session):
    """RIDE_CHARGE transaction stores reference_id pointing to a real ride.id."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    ride = Ride(
        user_id=user.id,
        idempotency_key=str(uuid.uuid4()),
        event_type="ACCEPTED",
        ride_data={"price": 25.0},
        ride_hash="a" * 64,
    )
    db_session.add(ride)
    await db_session.flush()

    tx = _make_transaction(
        user.id,
        type=TransactionType.RIDE_CHARGE.value,
        amount=-2,
        balance_after=8,
        reference_id=ride.id,
    )
    db_session.add(tx)
    await db_session.flush()

    result = await db_session.execute(
        select(CreditTransaction).where(CreditTransaction.id == tx.id)
    )
    loaded = result.scalar_one()
    assert loaded.reference_id == ride.id
    assert loaded.amount == -2


# --- Test strategy item 4: CASCADE delete — deleting User removes transactions ---


async def test_cascade_delete_on_user_removal(db_session):
    """Deleting a User automatically deletes all associated CreditTransactions."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    tx1 = _make_transaction(user.id, amount=10, balance_after=10)
    tx2 = _make_transaction(
        user.id,
        type=TransactionType.RIDE_CHARGE.value,
        amount=-2,
        balance_after=8,
    )
    db_session.add_all([tx1, tx2])
    await db_session.flush()

    tx1_id, tx2_id = tx1.id, tx2.id

    await db_session.delete(user)
    await db_session.flush()

    result = await db_session.execute(
        select(CreditTransaction).where(CreditTransaction.id.in_([tx1_id, tx2_id]))
    )
    assert result.scalars().all() == []


# --- Test strategy item 5: application-level validation — invalid type string ---


def test_invalid_transaction_type_raises_value_error():
    """TransactionType enum rejects unknown type strings at Python level."""
    with pytest.raises(ValueError):
        TransactionType("INVALID_TYPE")


# --- Additional: nullable fields ---


async def test_reference_id_and_description_nullable(db_session):
    """reference_id and description default to None."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    tx = _make_transaction(user.id)
    db_session.add(tx)
    await db_session.flush()

    result = await db_session.execute(
        select(CreditTransaction).where(CreditTransaction.id == tx.id)
    )
    loaded = result.scalar_one()
    assert loaded.reference_id is None
    assert loaded.description is None


# --- Additional: description stored for ADMIN_ADJUSTMENT ---


async def test_admin_adjustment_with_description(db_session):
    """ADMIN_ADJUSTMENT transaction stores a description."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    tx = _make_transaction(
        user.id,
        type=TransactionType.ADMIN_ADJUSTMENT.value,
        amount=-5,
        balance_after=5,
        description="Refund for cancelled ride",
    )
    db_session.add(tx)
    await db_session.flush()

    result = await db_session.execute(
        select(CreditTransaction).where(CreditTransaction.id == tx.id)
    )
    loaded = result.scalar_one()
    assert loaded.description == "Refund for cancelled ride"


# --- Additional: negative amount allowed (for charges) ---


async def test_negative_amount_for_ride_charge(db_session):
    """amount can be negative (e.g. RIDE_CHARGE deductions)."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    tx = _make_transaction(
        user.id,
        type=TransactionType.RIDE_CHARGE.value,
        amount=-3,
        balance_after=7,
    )
    db_session.add(tx)
    await db_session.flush()

    result = await db_session.execute(
        select(CreditTransaction).where(CreditTransaction.id == tx.id)
    )
    loaded = result.scalar_one()
    assert loaded.amount == -3
    assert loaded.balance_after == 7
