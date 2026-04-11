import uuid

import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from app.models.purchase_order import PurchaseOrder, PurchaseStatus
from app.models.user import User


def _make_user(email: str = "test@example.com") -> User:
    return User(email=email, password_hash="hashed")


def _make_order(user_id: uuid.UUID, **overrides) -> PurchaseOrder:
    defaults = {
        "user_id": user_id,
        "product_id": "credits_50",
        "purchase_token": str(uuid.uuid4()),
        "credits_amount": 50,
    }
    defaults.update(overrides)
    return PurchaseOrder(**defaults)


# --- Test strategy item 1: create PurchaseOrder → success, status='PENDING' ---


async def test_create_purchase_order_success(db_session):
    """PurchaseOrder is created with default status PENDING."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    order = _make_order(user.id)
    db_session.add(order)
    await db_session.flush()

    result = await db_session.execute(select(PurchaseOrder).where(PurchaseOrder.id == order.id))
    loaded = result.scalar_one()
    assert loaded.status == PurchaseStatus.PENDING.value
    assert loaded.product_id == "credits_50"
    assert loaded.credits_amount == 50
    assert loaded.user_id == user.id
    assert loaded.id is not None
    assert loaded.created_at is not None
    assert loaded.google_order_id is None
    assert loaded.verified_at is None


# --- Test strategy item 2: UNIQUE constraint on purchase_token → IntegrityError ---


async def test_unique_constraint_on_purchase_token(db_session):
    """Duplicate purchase_token raises IntegrityError."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    token = "duplicate-token-value"
    order1 = _make_order(user.id, purchase_token=token)
    db_session.add(order1)
    await db_session.flush()

    order2 = _make_order(user.id, purchase_token=token)
    db_session.add(order2)
    with pytest.raises(IntegrityError):
        await db_session.flush()


# --- Test strategy item 3: UNIQUE constraint on google_order_id → IntegrityError ---


async def test_unique_constraint_on_google_order_id(db_session):
    """Duplicate google_order_id raises IntegrityError."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    goid = "GPA.1234-5678-9012-34567"
    order1 = _make_order(user.id, google_order_id=goid)
    db_session.add(order1)
    await db_session.flush()

    order2 = _make_order(user.id, google_order_id=goid)
    db_session.add(order2)
    with pytest.raises(IntegrityError):
        await db_session.flush()


# --- Test strategy item 4: application-level validation — invalid status → ValueError ---


def test_invalid_status_raises_value_error():
    """PurchaseStatus enum rejects unknown status strings at Python level."""
    with pytest.raises(ValueError):
        PurchaseStatus("INVALID_STATUS")


# --- Test strategy item 5: SET NULL on user deletion — PurchaseOrder preserved ---


async def test_soft_delete_sets_null_on_user_removal(db_session):
    """Deleting a User sets user_id to NULL on associated PurchaseOrders (SET NULL FK)."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    order = _make_order(user.id)
    db_session.add(order)
    await db_session.flush()

    order_id = order.id

    await db_session.delete(user)
    await db_session.flush()

    # Expire cached objects so re-query picks up DB-level SET NULL
    db_session.expire_all()

    result = await db_session.execute(select(PurchaseOrder).where(PurchaseOrder.id == order_id))
    orphaned = result.scalar_one_or_none()
    assert orphaned is not None
    assert orphaned.user_id is None


# --- Additional: google_order_id allows multiple NULLs (UNIQUE ignores NULLs in PG) ---


async def test_multiple_null_google_order_ids(db_session):
    """Multiple PurchaseOrders with NULL google_order_id are allowed (PG UNIQUE ignores NULLs)."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    order1 = _make_order(user.id, google_order_id=None)
    order2 = _make_order(user.id, google_order_id=None)
    db_session.add_all([order1, order2])
    await db_session.flush()

    result = await db_session.execute(
        select(PurchaseOrder).where(PurchaseOrder.user_id == user.id)
    )
    assert len(result.scalars().all()) == 2


# --- Additional: CHECK constraint — credits_amount must be > 0 ---


async def test_check_constraint_credits_amount_positive(db_session):
    """credits_amount <= 0 raises IntegrityError (CHECK constraint)."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    order = _make_order(user.id, credits_amount=0)
    db_session.add(order)
    with pytest.raises(IntegrityError):
        await db_session.flush()


# --- Additional: relationship user.purchase_orders ---


async def test_user_purchase_orders_relationship(db_session):
    """user.purchase_orders returns the list of PurchaseOrders."""
    user = _make_user()
    db_session.add(user)
    await db_session.flush()

    order = _make_order(user.id, google_order_id="GPA.order-1")
    db_session.add(order)
    await db_session.flush()

    await db_session.refresh(user, ["purchase_orders"])
    assert len(user.purchase_orders) == 1
    assert user.purchase_orders[0].product_id == "credits_50"
