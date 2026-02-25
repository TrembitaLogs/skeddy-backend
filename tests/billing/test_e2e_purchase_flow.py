"""E2E tests for Purchase flow with idempotent replay.

Verifies the complete purchase flow: Google Play verification, credit
application, PurchaseOrder lifecycle, and idempotency handling.

Test strategy (task 14.3):
1. Verify all assertions with mock Google Play API
2. Test idempotency: two requests with same token -> one order
3. Test error handling: mock Google API failure
4. Verify Google API mock called with correct parameters
"""

from unittest.mock import patch

from sqlalchemy import func, select

from app.models.credit_transaction import CreditTransaction, TransactionType
from app.models.purchase_order import PurchaseOrder, PurchaseStatus
from tests.billing.conftest import make_google_play_mock
from tests.billing.helpers import assert_balance, assert_transaction_exists

PURCHASE_URL = "/api/v1/credits/purchase"


async def test_purchase_increases_balance(authenticated_client, billing_app_config, db_session):
    """POST /credits/purchase -> balance increases by product credits amount."""
    auth = authenticated_client
    mock_svc = make_google_play_mock()

    with patch("app.routers.credits._get_google_play_service", return_value=mock_svc):
        resp = await auth.client.post(
            PURCHASE_URL,
            json={
                "product_id": "credits_50",
                "purchase_token": "e2e-balance-token",
            },
            headers=auth.headers,
        )

    assert resp.status_code == 201
    data = resp.json()
    assert data["credits_added"] == 50
    assert data["new_balance"] == 60  # 10 (registration bonus) + 50

    # Verify DB balance matches response
    await assert_balance(db_session, auth.user_id, 60)

    # Verify Google API called with correct parameters (test strategy point 4)
    mock_svc.verify_purchase.assert_called_once_with("credits_50", "e2e-balance-token")
    mock_svc.consume_purchase.assert_called_once_with("credits_50", "e2e-balance-token")


async def test_purchase_creates_transaction(authenticated_client, billing_app_config, db_session):
    """POST /credits/purchase -> CreditTransaction(PURCHASE) with reference_id to PurchaseOrder."""
    auth = authenticated_client
    mock_svc = make_google_play_mock()

    with patch("app.routers.credits._get_google_play_service", return_value=mock_svc):
        resp = await auth.client.post(
            PURCHASE_URL,
            json={
                "product_id": "credits_10",
                "purchase_token": "e2e-tx-token",
            },
            headers=auth.headers,
        )

    assert resp.status_code == 201

    # Verify PURCHASE transaction exists with correct fields
    tx = await assert_transaction_exists(db_session, auth.user_id, TransactionType.PURCHASE, 10)
    assert tx.balance_after == 20  # 10 (bonus) + 10 (purchase)
    assert tx.reference_id is not None

    # reference_id points to the PurchaseOrder
    result = await db_session.execute(
        select(PurchaseOrder).where(PurchaseOrder.purchase_token == "e2e-tx-token")
    )
    order = result.scalar_one()
    assert tx.reference_id == order.id


async def test_purchase_creates_verified_order(
    authenticated_client, billing_app_config, db_session
):
    """POST /credits/purchase -> PurchaseOrder status=VERIFIED with google_order_id."""
    auth = authenticated_client
    mock_svc = make_google_play_mock()

    with patch("app.routers.credits._get_google_play_service", return_value=mock_svc):
        resp = await auth.client.post(
            PURCHASE_URL,
            json={
                "product_id": "credits_25",
                "purchase_token": "e2e-order-token",
            },
            headers=auth.headers,
        )

    assert resp.status_code == 201

    result = await db_session.execute(
        select(PurchaseOrder).where(PurchaseOrder.purchase_token == "e2e-order-token")
    )
    order = result.scalar_one()
    assert order.status == PurchaseStatus.VERIFIED.value
    assert order.google_order_id == "GPA.test-order-001"  # default mock order_id
    assert order.credits_amount == 25
    assert order.product_id == "credits_25"
    assert order.verified_at is not None


async def test_purchase_idempotent_replay(authenticated_client, billing_app_config, db_session):
    """Repeat request with same purchase_token -> 200 idempotent, no double crediting."""
    auth = authenticated_client
    mock_svc = make_google_play_mock()

    with patch("app.routers.credits._get_google_play_service", return_value=mock_svc):
        # First purchase
        resp1 = await auth.client.post(
            PURCHASE_URL,
            json={
                "product_id": "credits_10",
                "purchase_token": "e2e-idempotent-token",
            },
            headers=auth.headers,
        )
        assert resp1.status_code == 201
        assert resp1.json()["new_balance"] == 20  # 10 (bonus) + 10

        # Second purchase with same token
        resp2 = await auth.client.post(
            PURCHASE_URL,
            json={
                "product_id": "credits_10",
                "purchase_token": "e2e-idempotent-token",
            },
            headers=auth.headers,
        )
        assert resp2.status_code == 200  # Idempotent replay, not 201

    data2 = resp2.json()
    assert data2["credits_added"] == 10
    assert data2["new_balance"] == 20  # Balance unchanged

    # Balance NOT increased by second request
    await assert_balance(db_session, auth.user_id, 20)

    # Only one PurchaseOrder created
    result = await db_session.execute(
        select(func.count())
        .select_from(PurchaseOrder)
        .where(PurchaseOrder.user_id == auth.user_id)
    )
    assert result.scalar_one() == 1

    # Only one PURCHASE transaction (not counting REGISTRATION_BONUS)
    result = await db_session.execute(
        select(func.count())
        .select_from(CreditTransaction)
        .where(
            CreditTransaction.user_id == auth.user_id,
            CreditTransaction.type == TransactionType.PURCHASE,
        )
    )
    assert result.scalar_one() == 1

    # Google API called only once (first request only)
    mock_svc.verify_purchase.assert_called_once()
    mock_svc.consume_purchase.assert_called_once()


async def test_purchase_google_api_failure(authenticated_client, billing_app_config, db_session):
    """Google API unavailable -> 503 SERVICE_UNAVAILABLE, order FAILED, balance unchanged."""
    auth = authenticated_client
    mock_svc = make_google_play_mock(verify_error=Exception("Connection timeout"))

    with patch("app.routers.credits._get_google_play_service", return_value=mock_svc):
        resp = await auth.client.post(
            PURCHASE_URL,
            json={
                "product_id": "credits_10",
                "purchase_token": "e2e-failure-token",
            },
            headers=auth.headers,
        )

    assert resp.status_code == 503
    assert resp.json()["error"]["code"] == "SERVICE_UNAVAILABLE"

    # Balance unchanged (still just registration bonus)
    await assert_balance(db_session, auth.user_id, 10)

    # PurchaseOrder created but FAILED
    result = await db_session.execute(
        select(PurchaseOrder).where(PurchaseOrder.purchase_token == "e2e-failure-token")
    )
    order = result.scalar_one()
    assert order.status == PurchaseStatus.FAILED.value
