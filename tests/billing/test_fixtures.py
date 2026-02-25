"""Tests verifying that billing fixtures and factories work correctly.

Test strategy (task 14.1):
1. mock_google_play_service fixture creates without errors
2. user_with_balance has CreditBalance.balance=10
3. Factories create valid models with correct constraints
4. billing_app_config fixture has all required keys
"""

from sqlalchemy import select

from app.models.app_config import AppConfig
from app.models.credit_transaction import TransactionType
from app.models.purchase_order import PurchaseOrder, PurchaseStatus
from tests.billing.helpers import assert_balance, assert_transaction_exists, make_ride_hash
from tests.factories import (
    CreditBalanceFactory,
    CreditTransactionFactory,
    PurchaseOrderFactory,
)

# ---------------------------------------------------------------------------
# Test 1: mock_google_play_service fixture creates without errors
# ---------------------------------------------------------------------------


async def test_mock_google_play_service_creates(mock_google_play_service):
    """mock_google_play_service fixture provides working verify/consume mocks."""
    svc = mock_google_play_service

    result = await svc.verify_purchase("credits_10", "test-token")
    assert result.order_id == "GPA.test-order-001"
    assert result.purchase_state == 0
    assert result.already_consumed is False

    consume_ok = await svc.consume_purchase("credits_10", "test-token")
    assert consume_ok is True


async def test_mock_google_play_service_tracks_calls(mock_google_play_service):
    """Mock methods are AsyncMock — call assertions work."""
    svc = mock_google_play_service

    await svc.verify_purchase("credits_50", "token-abc")
    svc.verify_purchase.assert_called_once_with("credits_50", "token-abc")

    await svc.consume_purchase("credits_50", "token-abc")
    svc.consume_purchase.assert_called_once_with("credits_50", "token-abc")


# ---------------------------------------------------------------------------
# Test 2: user_with_balance has CreditBalance.balance=10
# ---------------------------------------------------------------------------


async def test_user_with_balance_has_correct_balance(db_session, user_with_balance):
    """user_with_balance fixture creates a user with balance=10."""
    await assert_balance(db_session, user_with_balance.id, 10)


async def test_user_with_balance_has_registration_bonus_tx(db_session, user_with_balance):
    """user_with_balance fixture creates REGISTRATION_BONUS transaction."""
    await assert_transaction_exists(
        db_session,
        user_with_balance.id,
        TransactionType.REGISTRATION_BONUS,
        10,
    )


async def test_user_zero_balance_has_zero(db_session, user_zero_balance):
    """user_zero_balance fixture creates a user with balance=0."""
    await assert_balance(db_session, user_zero_balance.id, 0)


# ---------------------------------------------------------------------------
# Test 3: Factories create valid models with correct constraints
# ---------------------------------------------------------------------------


async def test_credit_balance_factory_builds_valid_model(db_session, user_with_balance):
    """CreditBalanceFactory.build() creates a model with expected defaults."""
    cb = CreditBalanceFactory.build(user_id=user_with_balance.id, balance=25)
    assert cb.balance == 25
    assert cb.user_id == user_with_balance.id


async def test_credit_transaction_factory_builds_valid_model(db_session, user_with_balance):
    """CreditTransactionFactory.build() creates a model with expected defaults."""
    tx = CreditTransactionFactory.build(
        user_id=user_with_balance.id,
        type=TransactionType.RIDE_CHARGE,
        amount=-2,
        balance_after=8,
    )
    assert tx.type == TransactionType.RIDE_CHARGE
    assert tx.amount == -2
    assert tx.balance_after == 8


async def test_purchase_order_factory_builds_valid_model(db_session, user_with_balance):
    """PurchaseOrderFactory.build() creates a model with expected defaults."""
    po = PurchaseOrderFactory.build(user_id=user_with_balance.id)
    assert po.product_id == "credits_10"
    assert po.credits_amount == 10
    assert po.status == PurchaseStatus.PENDING.value
    assert po.google_order_id is None
    assert po.verified_at is None
    assert po.purchase_token is not None


async def test_purchase_order_factory_persists_to_db(db_session, user_with_balance):
    """PurchaseOrderFactory model can be persisted and read back."""
    po = PurchaseOrderFactory.build(user_id=user_with_balance.id)
    db_session.add(po)
    await db_session.flush()

    result = await db_session.execute(select(PurchaseOrder).where(PurchaseOrder.id == po.id))
    saved = result.scalar_one()
    assert saved.purchase_token == po.purchase_token
    assert saved.credits_amount == 10


async def test_purchase_order_factory_unique_tokens():
    """Each PurchaseOrderFactory.build() generates a unique purchase_token."""
    po1 = PurchaseOrderFactory.build()
    po2 = PurchaseOrderFactory.build()
    assert po1.purchase_token != po2.purchase_token


# ---------------------------------------------------------------------------
# Test 4: billing_app_config fixture has all required keys
# ---------------------------------------------------------------------------


async def test_billing_app_config_has_all_keys(db_session, billing_app_config):
    """billing_app_config fixture seeds all required AppConfig keys."""
    expected_keys = {
        "credit_products",
        "ride_credit_tiers",
        "registration_bonus_credits",
        "verification_deadline_minutes",
        "verification_check_interval_minutes",
    }

    result = await db_session.execute(
        select(AppConfig.key).where(AppConfig.key.in_(expected_keys))
    )
    found_keys = {row[0] for row in result.all()}

    assert found_keys == expected_keys


async def test_billing_app_config_credit_products_valid(db_session, billing_app_config):
    """billing_app_config seeds valid credit_products JSON."""
    import json

    result = await db_session.execute(
        select(AppConfig.value).where(AppConfig.key == "credit_products")
    )
    raw = result.scalar_one()
    products = json.loads(raw)

    assert len(products) == 4
    product_ids = {p["product_id"] for p in products}
    assert product_ids == {"credits_10", "credits_25", "credits_50", "credits_100"}

    for p in products:
        assert p["credits"] > 0
        assert p["price_usd"] > 0


async def test_billing_app_config_ride_credit_tiers_valid(db_session, billing_app_config):
    """billing_app_config seeds valid ride_credit_tiers JSON with catch-all."""
    import json

    result = await db_session.execute(
        select(AppConfig.value).where(AppConfig.key == "ride_credit_tiers")
    )
    raw = result.scalar_one()
    tiers = json.loads(raw)

    assert len(tiers) == 3
    # Last tier must be catch-all (max_price=null)
    assert tiers[-1]["max_price"] is None
    assert tiers[-1]["credits"] > 0


# ---------------------------------------------------------------------------
# Bonus: make_ride_hash helper
# ---------------------------------------------------------------------------


def test_make_ride_hash_length():
    """make_ride_hash generates a 64-character hex string."""
    h = make_ride_hash()
    assert len(h) == 64
    # Verify it's valid hex
    int(h, 16)


def test_make_ride_hash_unique():
    """Each call to make_ride_hash produces a unique value."""
    hashes = {make_ride_hash() for _ in range(10)}
    assert len(hashes) == 10
