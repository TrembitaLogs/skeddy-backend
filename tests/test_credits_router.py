"""Tests for POST /api/v1/credits/purchase endpoint (tasks 4.5, 4.6, 4.7).

Test strategy (4.5):
1. Integration test: valid request -> 201 with credits_added
2. Unknown product_id -> 400 UNKNOWN_PRODUCT
3. Invalid token -> 400 INVALID_PURCHASE_TOKEN
4. Google API 503 -> 503 response
5. Rate limit: covered by existing rate limiter test infrastructure

Test strategy (4.6 — idempotency & recovery):
16. VERIFIED token -> 200 idempotent replay with existing balance
17. CONSUMED recovery -> 201, credits applied without Google calls
18. google_order_id deduplication -> 200 when duplicate VERIFIED exists
19. FAILED recovery -> 201, retry succeeds through full flow
20. already_consumed from Google -> 201, consume() skipped
21. CONSUMED recovery -> CreditTransaction(PURCHASE) created correctly

Test strategy (4.7 — race conditions & IntegrityError handling):
22. purchase_token IntegrityError -> rollback -> re-read VERIFIED -> 200
23. purchase_token IntegrityError -> rollback -> re-read PENDING -> proceed -> 201
24. google_order_id IntegrityError on consume commit -> rollback -> 200
"""

import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

from sqlalchemy import select

from app.models.app_config import AppConfig
from app.models.credit_balance import CreditBalance
from app.models.credit_transaction import CreditTransaction, TransactionType
from app.models.purchase_order import PurchaseOrder, PurchaseStatus
from app.services.google_play_service import (
    GooglePlayVerificationError,
    GooglePurchaseResult,
)

PURCHASE_URL = "/api/v1/credits/purchase"

CREDIT_PRODUCTS_JSON = json.dumps(
    [
        {"product_id": "credits_10", "credits": 10, "price_usd": 10.00},
        {"product_id": "credits_25", "credits": 25, "price_usd": 22.00},
        {"product_id": "credits_50", "credits": 50, "price_usd": 40.00},
        {"product_id": "credits_100", "credits": 100, "price_usd": 80.00},
    ]
)


def _mock_gp_service(
    verify_result: GooglePurchaseResult | None = None,
    verify_error: Exception | None = None,
    consume_result: bool = True,
) -> MagicMock:
    """Create a mock GooglePlayService with configurable behavior."""
    svc = MagicMock()
    if verify_error:
        svc.verify_purchase = AsyncMock(side_effect=verify_error)
    else:
        svc.verify_purchase = AsyncMock(
            return_value=verify_result
            or GooglePurchaseResult(
                order_id="GPA.1234-5678-9012-34567",
                purchase_state=0,
                consumption_state=0,
                acknowledgement_state=0,
                purchase_time_millis="1708700000000",
                already_consumed=False,
            )
        )
    svc.consume_purchase = AsyncMock(return_value=consume_result)
    return svc


# ---------------------------------------------------------------------------
# Test 1: Valid purchase -> 201 with credits_added and new_balance
# ---------------------------------------------------------------------------


async def test_purchase_valid_request_returns_201(authenticated_client, db_session, fake_redis):
    """POST /credits/purchase with valid data -> 201, credits added to balance."""
    auth = authenticated_client

    # Seed credit products in AppConfig
    db_session.add(AppConfig(key="credit_products", value=CREDIT_PRODUCTS_JSON))
    await db_session.flush()

    mock_svc = _mock_gp_service()

    with patch("app.routers.credits._create_google_play_service", return_value=mock_svc):
        resp = await auth.client.post(
            PURCHASE_URL,
            json={
                "product_id": "credits_50",
                "purchase_token": "valid-token-abc",
            },
            headers=auth.headers,
        )

    assert resp.status_code == 201
    data = resp.json()
    assert data["credits_added"] == 50
    # Registration bonus (10) + purchase (50) = 60
    assert data["new_balance"] == 60

    # Verify Google API was called correctly
    mock_svc.verify_purchase.assert_called_once_with("credits_50", "valid-token-abc")
    mock_svc.consume_purchase.assert_called_once_with("credits_50", "valid-token-abc")


# ---------------------------------------------------------------------------
# Test 2: PurchaseOrder created and finalized as VERIFIED
# ---------------------------------------------------------------------------


async def test_purchase_creates_verified_order(authenticated_client, db_session, fake_redis):
    """Purchase creates a PurchaseOrder with status VERIFIED after success."""
    auth = authenticated_client

    db_session.add(AppConfig(key="credit_products", value=CREDIT_PRODUCTS_JSON))
    await db_session.flush()

    mock_svc = _mock_gp_service()

    with patch("app.routers.credits._create_google_play_service", return_value=mock_svc):
        resp = await auth.client.post(
            PURCHASE_URL,
            json={
                "product_id": "credits_10",
                "purchase_token": "token-order-check",
            },
            headers=auth.headers,
        )

    assert resp.status_code == 201

    result = await db_session.execute(
        select(PurchaseOrder).where(PurchaseOrder.purchase_token == "token-order-check")
    )
    order = result.scalar_one()
    assert order.status == PurchaseStatus.VERIFIED.value
    assert order.google_order_id == "GPA.1234-5678-9012-34567"
    assert order.credits_amount == 10
    assert order.verified_at is not None


# ---------------------------------------------------------------------------
# Test 3: CreditTransaction PURCHASE created with correct fields
# ---------------------------------------------------------------------------


async def test_purchase_creates_credit_transaction(authenticated_client, db_session, fake_redis):
    """Purchase creates a CreditTransaction(PURCHASE) with reference_id = order.id."""
    auth = authenticated_client

    db_session.add(AppConfig(key="credit_products", value=CREDIT_PRODUCTS_JSON))
    await db_session.flush()

    mock_svc = _mock_gp_service()

    with patch("app.routers.credits._create_google_play_service", return_value=mock_svc):
        resp = await auth.client.post(
            PURCHASE_URL,
            json={
                "product_id": "credits_25",
                "purchase_token": "token-tx-check",
            },
            headers=auth.headers,
        )

    assert resp.status_code == 201

    # Find the PURCHASE transaction (not the REGISTRATION_BONUS)
    result = await db_session.execute(
        select(CreditTransaction).where(
            CreditTransaction.user_id == auth.user_id,
            CreditTransaction.type == TransactionType.PURCHASE,
        )
    )
    tx = result.scalar_one()
    assert tx.amount == 25
    assert tx.balance_after == 35  # 10 (bonus) + 25 (purchase)
    assert tx.reference_id is not None

    # Verify reference_id points to the PurchaseOrder
    order_result = await db_session.execute(
        select(PurchaseOrder).where(PurchaseOrder.purchase_token == "token-tx-check")
    )
    order = order_result.scalar_one()
    assert tx.reference_id == order.id


# ---------------------------------------------------------------------------
# Test 4: Unknown product_id -> 400 UNKNOWN_PRODUCT
# ---------------------------------------------------------------------------


async def test_purchase_unknown_product_returns_400(authenticated_client, db_session, fake_redis):
    """POST /credits/purchase with unknown product_id -> 400 UNKNOWN_PRODUCT."""
    auth = authenticated_client

    db_session.add(AppConfig(key="credit_products", value=CREDIT_PRODUCTS_JSON))
    await db_session.flush()

    resp = await auth.client.post(
        PURCHASE_URL,
        json={
            "product_id": "credits_999",
            "purchase_token": "some-token",
        },
        headers=auth.headers,
    )

    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "UNKNOWN_PRODUCT"


# ---------------------------------------------------------------------------
# Test 5: Missing credit_products config -> 400 UNKNOWN_PRODUCT
# ---------------------------------------------------------------------------


async def test_purchase_missing_config_returns_400(authenticated_client, db_session, fake_redis):
    """Product not in catalog (including defaults) -> 400 UNKNOWN_PRODUCT."""
    auth = authenticated_client

    resp = await auth.client.post(
        PURCHASE_URL,
        json={
            "product_id": "nonexistent_product",
            "purchase_token": "some-token",
        },
        headers=auth.headers,
    )

    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "UNKNOWN_PRODUCT"


# ---------------------------------------------------------------------------
# Test 6: Invalid purchase token -> 400 INVALID_PURCHASE_TOKEN
# ---------------------------------------------------------------------------


async def test_purchase_invalid_token_returns_400(authenticated_client, db_session, fake_redis):
    """Google Play verification error -> 400 INVALID_PURCHASE_TOKEN."""
    auth = authenticated_client

    db_session.add(AppConfig(key="credit_products", value=CREDIT_PRODUCTS_JSON))
    await db_session.flush()

    mock_svc = _mock_gp_service(
        verify_error=GooglePlayVerificationError(
            code="INVALID_PURCHASE_TOKEN",
            message="Purchase token not found",
        )
    )

    with patch("app.routers.credits._create_google_play_service", return_value=mock_svc):
        resp = await auth.client.post(
            PURCHASE_URL,
            json={
                "product_id": "credits_10",
                "purchase_token": "bad-token",
            },
            headers=auth.headers,
        )

    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "INVALID_PURCHASE_TOKEN"

    # Verify PurchaseOrder was set to FAILED
    result = await db_session.execute(
        select(PurchaseOrder).where(PurchaseOrder.purchase_token == "bad-token")
    )
    order = result.scalar_one()
    assert order.status == PurchaseStatus.FAILED.value


# ---------------------------------------------------------------------------
# Test 7: Google API unavailable during verify -> 503 SERVICE_UNAVAILABLE
# ---------------------------------------------------------------------------


async def test_purchase_google_api_unavailable_verify_returns_503(
    authenticated_client, db_session, fake_redis
):
    """Google API error during verification -> 503 SERVICE_UNAVAILABLE."""
    auth = authenticated_client

    db_session.add(AppConfig(key="credit_products", value=CREDIT_PRODUCTS_JSON))
    await db_session.flush()

    mock_svc = _mock_gp_service(verify_error=OSError("Connection timeout"))

    with patch("app.routers.credits._create_google_play_service", return_value=mock_svc):
        resp = await auth.client.post(
            PURCHASE_URL,
            json={
                "product_id": "credits_10",
                "purchase_token": "timeout-token",
            },
            headers=auth.headers,
        )

    assert resp.status_code == 503
    assert resp.json()["error"]["code"] == "SERVICE_UNAVAILABLE"

    # Verify PurchaseOrder was set to FAILED
    result = await db_session.execute(
        select(PurchaseOrder).where(PurchaseOrder.purchase_token == "timeout-token")
    )
    order = result.scalar_one()
    assert order.status == PurchaseStatus.FAILED.value


# ---------------------------------------------------------------------------
# Test 8: Google API consume fails -> 503 SERVICE_UNAVAILABLE
# ---------------------------------------------------------------------------


async def test_purchase_consume_fails_returns_503(authenticated_client, db_session, fake_redis):
    """Google API consume failure -> 503, order set to FAILED."""
    auth = authenticated_client

    db_session.add(AppConfig(key="credit_products", value=CREDIT_PRODUCTS_JSON))
    await db_session.flush()

    mock_svc = _mock_gp_service(consume_result=False)

    with patch("app.routers.credits._create_google_play_service", return_value=mock_svc):
        resp = await auth.client.post(
            PURCHASE_URL,
            json={
                "product_id": "credits_10",
                "purchase_token": "consume-fail-token",
            },
            headers=auth.headers,
        )

    assert resp.status_code == 503
    assert resp.json()["error"]["code"] == "SERVICE_UNAVAILABLE"

    result = await db_session.execute(
        select(PurchaseOrder).where(PurchaseOrder.purchase_token == "consume-fail-token")
    )
    order = result.scalar_one()
    assert order.status == PurchaseStatus.FAILED.value


# ---------------------------------------------------------------------------
# Test 9: Redis cache populated after product lookup
# ---------------------------------------------------------------------------


async def test_purchase_caches_credit_products_in_redis(
    authenticated_client, db_session, fake_redis
):
    """Product catalog is cached in Redis after first DB lookup."""
    auth = authenticated_client

    db_session.add(AppConfig(key="credit_products", value=CREDIT_PRODUCTS_JSON))
    await db_session.flush()

    mock_svc = _mock_gp_service()

    with patch("app.routers.credits._create_google_play_service", return_value=mock_svc):
        resp = await auth.client.post(
            PURCHASE_URL,
            json={
                "product_id": "credits_10",
                "purchase_token": "cache-test-token",
            },
            headers=auth.headers,
        )

    assert resp.status_code == 201

    # Verify credit_products was cached in Redis
    cached = fake_redis._store.get("app_config:credit_products")
    assert cached is not None
    products = json.loads(cached)
    assert len(products) == 4
    assert any(p["product_id"] == "credits_10" for p in products)


# ---------------------------------------------------------------------------
# Test 10: Balance updated in Redis cache (write-through)
# ---------------------------------------------------------------------------


async def test_purchase_updates_redis_balance_cache(authenticated_client, db_session, fake_redis):
    """After purchase, Redis balance cache reflects the new balance."""
    auth = authenticated_client

    db_session.add(AppConfig(key="credit_products", value=CREDIT_PRODUCTS_JSON))
    await db_session.flush()

    mock_svc = _mock_gp_service()

    with patch("app.routers.credits._create_google_play_service", return_value=mock_svc):
        resp = await auth.client.post(
            PURCHASE_URL,
            json={
                "product_id": "credits_100",
                "purchase_token": "redis-balance-token",
            },
            headers=auth.headers,
        )

    assert resp.status_code == 201
    assert resp.json()["new_balance"] == 110  # 10 bonus + 100 purchase

    balance_key = f"user_balance:{auth.user_id}"
    assert fake_redis._store.get(balance_key) == "110"


# ---------------------------------------------------------------------------
# Test 11: CreditBalance row updated in DB
# ---------------------------------------------------------------------------


async def test_purchase_updates_db_balance(authenticated_client, db_session, fake_redis):
    """After purchase, CreditBalance row in DB has correct balance."""
    auth = authenticated_client

    db_session.add(AppConfig(key="credit_products", value=CREDIT_PRODUCTS_JSON))
    await db_session.flush()

    mock_svc = _mock_gp_service()

    with patch("app.routers.credits._create_google_play_service", return_value=mock_svc):
        resp = await auth.client.post(
            PURCHASE_URL,
            json={
                "product_id": "credits_50",
                "purchase_token": "db-balance-token",
            },
            headers=auth.headers,
        )

    assert resp.status_code == 201

    result = await db_session.execute(
        select(CreditBalance.balance).where(CreditBalance.user_id == auth.user_id)
    )
    assert result.scalar_one() == 60  # 10 bonus + 50 purchase


# ---------------------------------------------------------------------------
# Test 12: Unauthenticated request -> 401/403
# ---------------------------------------------------------------------------


async def test_purchase_unauthenticated_returns_error(app_client):
    """POST /credits/purchase without JWT -> 401."""
    resp = await app_client.post(
        PURCHASE_URL,
        json={
            "product_id": "credits_10",
            "purchase_token": "some-token",
        },
    )

    assert resp.status_code == 401


# ---------------------------------------------------------------------------
# Test 13: Empty product_id -> 422 validation error
# ---------------------------------------------------------------------------


async def test_purchase_empty_product_id_returns_422(authenticated_client):
    """POST /credits/purchase with empty product_id -> 422."""
    auth = authenticated_client

    resp = await auth.client.post(
        PURCHASE_URL,
        json={
            "product_id": "",
            "purchase_token": "some-token",
        },
        headers=auth.headers,
    )

    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# Test 14: Empty purchase_token -> 422 validation error
# ---------------------------------------------------------------------------


async def test_purchase_empty_purchase_token_returns_422(authenticated_client):
    """POST /credits/purchase with empty purchase_token -> 422."""
    auth = authenticated_client

    resp = await auth.client.post(
        PURCHASE_URL,
        json={
            "product_id": "credits_10",
            "purchase_token": "",
        },
        headers=auth.headers,
    )

    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# Test 15: Different product tiers return correct credits
# ---------------------------------------------------------------------------


async def test_purchase_different_products_credit_correctly(
    authenticated_client, db_session, fake_redis
):
    """Each product_id maps to the correct credit amount."""
    auth = authenticated_client

    db_session.add(AppConfig(key="credit_products", value=CREDIT_PRODUCTS_JSON))
    await db_session.flush()

    mock_svc = _mock_gp_service()

    with patch("app.routers.credits._create_google_play_service", return_value=mock_svc):
        # Purchase credits_10
        resp = await auth.client.post(
            PURCHASE_URL,
            json={
                "product_id": "credits_10",
                "purchase_token": "token-tier-10",
            },
            headers=auth.headers,
        )

    assert resp.status_code == 201
    assert resp.json()["credits_added"] == 10
    assert resp.json()["new_balance"] == 20  # 10 bonus + 10 purchase


# ===========================================================================
# Task 4.6 — Idempotency & recovery tests
# ===========================================================================


# ---------------------------------------------------------------------------
# Test 16: VERIFIED idempotent replay -> 200 with existing balance
# ---------------------------------------------------------------------------


async def test_purchase_verified_idempotent_returns_200(
    authenticated_client, db_session, fake_redis
):
    """Repeat request with already-VERIFIED purchase_token returns 200."""
    auth = authenticated_client

    db_session.add(AppConfig(key="credit_products", value=CREDIT_PRODUCTS_JSON))

    # Simulate previously credited purchase: balance = 10 bonus + 50 purchase
    balance_result = await db_session.execute(
        select(CreditBalance).where(CreditBalance.user_id == auth.user_id)
    )
    balance_row = balance_result.scalar_one()
    balance_row.balance = 60
    fake_redis._store[f"user_balance:{auth.user_id}"] = "60"

    order = PurchaseOrder(
        user_id=auth.user_id,
        product_id="credits_50",
        purchase_token="already-verified-token",
        credits_amount=50,
        status=PurchaseStatus.VERIFIED.value,
        google_order_id="GPA.verified-123",
        verified_at=datetime.now(UTC),
    )
    db_session.add(order)
    await db_session.flush()

    # POST with same purchase_token — Google API should NOT be called
    resp = await auth.client.post(
        PURCHASE_URL,
        json={
            "product_id": "credits_50",
            "purchase_token": "already-verified-token",
        },
        headers=auth.headers,
    )

    assert resp.status_code == 200
    data = resp.json()
    assert data["credits_added"] == 50
    assert data["new_balance"] == 60

    # Verify balance unchanged in DB
    balance_result = await db_session.execute(
        select(CreditBalance.balance).where(CreditBalance.user_id == auth.user_id)
    )
    assert balance_result.scalar_one() == 60


# ---------------------------------------------------------------------------
# Test 17: CONSUMED recovery -> 201, credits applied without Google calls
# ---------------------------------------------------------------------------


async def test_purchase_consumed_recovery_credits_applied(
    authenticated_client, db_session, fake_redis
):
    """CONSUMED order recovery applies credits without calling Google API."""
    auth = authenticated_client

    db_session.add(AppConfig(key="credit_products", value=CREDIT_PRODUCTS_JSON))

    # Create CONSUMED order (crash after consume, before crediting)
    order = PurchaseOrder(
        user_id=auth.user_id,
        product_id="credits_25",
        purchase_token="consumed-recovery-token",
        credits_amount=25,
        status=PurchaseStatus.CONSUMED.value,
        google_order_id="GPA.consumed-123",
    )
    db_session.add(order)
    await db_session.flush()

    # POST — recovery completes without any Google API calls
    resp = await auth.client.post(
        PURCHASE_URL,
        json={
            "product_id": "credits_25",
            "purchase_token": "consumed-recovery-token",
        },
        headers=auth.headers,
    )

    assert resp.status_code == 201
    data = resp.json()
    assert data["credits_added"] == 25
    assert data["new_balance"] == 35  # 10 bonus + 25 recovery

    # Verify order transitioned to VERIFIED
    result = await db_session.execute(
        select(PurchaseOrder).where(PurchaseOrder.purchase_token == "consumed-recovery-token")
    )
    verified_order = result.scalar_one()
    assert verified_order.status == PurchaseStatus.VERIFIED.value
    assert verified_order.verified_at is not None


# ---------------------------------------------------------------------------
# Test 18: google_order_id deduplication -> 200
# ---------------------------------------------------------------------------


async def test_purchase_google_order_id_dedup_returns_200(
    authenticated_client, db_session, fake_redis
):
    """Second token with same google_order_id returns 200 (dedup)."""
    auth = authenticated_client

    db_session.add(AppConfig(key="credit_products", value=CREDIT_PRODUCTS_JSON))

    # Simulate previously credited purchase
    balance_result = await db_session.execute(
        select(CreditBalance).where(CreditBalance.user_id == auth.user_id)
    )
    balance_row = balance_result.scalar_one()
    balance_row.balance = 60  # 10 bonus + 50 already credited
    fake_redis._store[f"user_balance:{auth.user_id}"] = "60"

    order1 = PurchaseOrder(
        user_id=auth.user_id,
        product_id="credits_50",
        purchase_token="first-token-dedup",
        credits_amount=50,
        status=PurchaseStatus.VERIFIED.value,
        google_order_id="GPA.shared-order-id",
        verified_at=datetime.now(UTC),
    )
    db_session.add(order1)
    await db_session.flush()

    # Mock Google to return the SAME google_order_id for a different token
    mock_svc = _mock_gp_service(
        verify_result=GooglePurchaseResult(
            order_id="GPA.shared-order-id",
            purchase_state=0,
            consumption_state=0,
            acknowledgement_state=0,
            purchase_time_millis="1708700000000",
            already_consumed=False,
        )
    )

    with patch("app.routers.credits._create_google_play_service", return_value=mock_svc):
        resp = await auth.client.post(
            PURCHASE_URL,
            json={
                "product_id": "credits_50",
                "purchase_token": "second-token-dedup",
            },
            headers=auth.headers,
        )

    assert resp.status_code == 200
    data = resp.json()
    assert data["credits_added"] == 50
    assert data["new_balance"] == 60  # No new credits added

    # Google verify was called (different token), but consume was NOT
    mock_svc.verify_purchase.assert_called_once()
    mock_svc.consume_purchase.assert_not_called()


# ---------------------------------------------------------------------------
# Test 19: FAILED recovery -> retry succeeds with 201
# ---------------------------------------------------------------------------


async def test_purchase_failed_recovery_succeeds(authenticated_client, db_session, fake_redis):
    """Retry after FAILED order succeeds and credits are applied."""
    auth = authenticated_client

    db_session.add(AppConfig(key="credit_products", value=CREDIT_PRODUCTS_JSON))

    # Create pre-existing FAILED order
    order = PurchaseOrder(
        user_id=auth.user_id,
        product_id="credits_25",
        purchase_token="failed-retry-token",
        credits_amount=25,
        status=PurchaseStatus.FAILED.value,
    )
    db_session.add(order)
    await db_session.flush()

    mock_svc = _mock_gp_service()

    with patch("app.routers.credits._create_google_play_service", return_value=mock_svc):
        resp = await auth.client.post(
            PURCHASE_URL,
            json={
                "product_id": "credits_25",
                "purchase_token": "failed-retry-token",
            },
            headers=auth.headers,
        )

    assert resp.status_code == 201
    data = resp.json()
    assert data["credits_added"] == 25
    assert data["new_balance"] == 35  # 10 bonus + 25 purchase

    # Verify order transitioned to VERIFIED (reused, not a new row)
    result = await db_session.execute(
        select(PurchaseOrder).where(PurchaseOrder.purchase_token == "failed-retry-token")
    )
    verified_order = result.scalar_one()
    assert verified_order.status == PurchaseStatus.VERIFIED.value
    assert verified_order.google_order_id == "GPA.1234-5678-9012-34567"


# ---------------------------------------------------------------------------
# Test 20: already_consumed from Google -> consume() skipped, credits applied
# ---------------------------------------------------------------------------


async def test_purchase_already_consumed_skips_consume_call(
    authenticated_client, db_session, fake_redis
):
    """When Google reports already_consumed, skip consume() and apply credits."""
    auth = authenticated_client

    db_session.add(AppConfig(key="credit_products", value=CREDIT_PRODUCTS_JSON))

    mock_svc = _mock_gp_service(
        verify_result=GooglePurchaseResult(
            order_id="GPA.already-consumed-456",
            purchase_state=0,
            consumption_state=1,
            acknowledgement_state=0,
            purchase_time_millis="1708700000000",
            already_consumed=True,
        )
    )

    with patch("app.routers.credits._create_google_play_service", return_value=mock_svc):
        resp = await auth.client.post(
            PURCHASE_URL,
            json={
                "product_id": "credits_10",
                "purchase_token": "already-consumed-token",
            },
            headers=auth.headers,
        )

    assert resp.status_code == 201
    data = resp.json()
    assert data["credits_added"] == 10
    assert data["new_balance"] == 20  # 10 bonus + 10 purchase

    # verify_purchase was called, consume_purchase was NOT
    mock_svc.verify_purchase.assert_called_once()
    mock_svc.consume_purchase.assert_not_called()

    # Order is VERIFIED with correct google_order_id
    result = await db_session.execute(
        select(PurchaseOrder).where(PurchaseOrder.purchase_token == "already-consumed-token")
    )
    order = result.scalar_one()
    assert order.status == PurchaseStatus.VERIFIED.value
    assert order.google_order_id == "GPA.already-consumed-456"


# ---------------------------------------------------------------------------
# Test 21: CONSUMED recovery creates correct CreditTransaction(PURCHASE)
# ---------------------------------------------------------------------------


async def test_purchase_consumed_recovery_creates_transaction(
    authenticated_client, db_session, fake_redis
):
    """CONSUMED recovery creates CreditTransaction(PURCHASE) with reference_id."""
    auth = authenticated_client

    db_session.add(AppConfig(key="credit_products", value=CREDIT_PRODUCTS_JSON))

    order = PurchaseOrder(
        user_id=auth.user_id,
        product_id="credits_50",
        purchase_token="consumed-tx-token",
        credits_amount=50,
        status=PurchaseStatus.CONSUMED.value,
        google_order_id="GPA.consumed-tx-123",
    )
    db_session.add(order)
    await db_session.flush()

    resp = await auth.client.post(
        PURCHASE_URL,
        json={
            "product_id": "credits_50",
            "purchase_token": "consumed-tx-token",
        },
        headers=auth.headers,
    )

    assert resp.status_code == 201

    # Find the PURCHASE transaction (not the REGISTRATION_BONUS)
    result = await db_session.execute(
        select(CreditTransaction).where(
            CreditTransaction.user_id == auth.user_id,
            CreditTransaction.type == TransactionType.PURCHASE,
        )
    )
    tx = result.scalar_one()
    assert tx.amount == 50
    assert tx.balance_after == 60  # 10 (bonus) + 50 (recovery)

    # reference_id points to the PurchaseOrder
    order_result = await db_session.execute(
        select(PurchaseOrder).where(PurchaseOrder.purchase_token == "consumed-tx-token")
    )
    recovered_order = order_result.scalar_one()
    assert tx.reference_id == recovered_order.id


# ===========================================================================
# Task 4.7 — Race condition & IntegrityError handling tests
# ===========================================================================


# ---------------------------------------------------------------------------
# Test 22: purchase_token IntegrityError -> re-read VERIFIED -> 200
# ---------------------------------------------------------------------------


async def test_purchase_token_race_verified_returns_200(
    authenticated_client, db_session, fake_redis
):
    """IntegrityError on PurchaseOrder INSERT: concurrent request already
    verified this token -> rollback, re-read as VERIFIED, return 200.

    Mocks db.commit to raise IntegrityError on first call, then patches
    the SELECT to return a VERIFIED order.
    """
    from unittest.mock import AsyncMock
    from unittest.mock import patch as mock_patch

    from sqlalchemy.exc import IntegrityError as SAIntegrityError

    auth = authenticated_client

    db_session.add(AppConfig(key="credit_products", value=CREDIT_PRODUCTS_JSON))
    await db_session.flush()

    # Pre-create a VERIFIED order that the "winning" concurrent request made
    verified_order = PurchaseOrder(
        user_id=auth.user_id,
        product_id="credits_10",
        purchase_token="race-token-verified",
        credits_amount=10,
        status=PurchaseStatus.VERIFIED.value,
        google_order_id="GPA.race-verified-123",
        verified_at=datetime.now(UTC),
    )
    db_session.add(verified_order)

    # Set balance to 20 (10 bonus + 10 from the "winning" request)
    balance_result = await db_session.execute(
        select(CreditBalance).where(CreditBalance.user_id == auth.user_id)
    )
    balance_row = balance_result.scalar_one()
    balance_row.balance = 20
    fake_redis._store[f"user_balance:{auth.user_id}"] = "20"
    await db_session.flush()

    commit_call_count = {"n": 0}
    original_commit = db_session.commit

    async def mock_commit():
        commit_call_count["n"] += 1
        if commit_call_count["n"] == 1:
            # First commit (INSERT PurchaseOrder) — simulate race
            raise SAIntegrityError("duplicate key", {}, None)
        return await original_commit()

    with (
        mock_patch.object(db_session, "commit", side_effect=mock_commit),
        mock_patch.object(db_session, "rollback", new_callable=AsyncMock),
    ):
        resp = await auth.client.post(
            PURCHASE_URL,
            json={
                "product_id": "credits_10",
                "purchase_token": "race-token-verified",
            },
            headers=auth.headers,
        )

    assert resp.status_code == 200
    data = resp.json()
    assert data["credits_added"] == 10
    assert data["new_balance"] == 20


# ---------------------------------------------------------------------------
# Test 23: purchase_token IntegrityError -> re-read PENDING -> proceed -> 201
# ---------------------------------------------------------------------------


async def test_purchase_token_race_pending_proceeds_to_verify(
    authenticated_client, db_session, fake_redis
):
    """IntegrityError on INSERT: concurrent request created PENDING order ->
    rollback, re-read as PENDING, proceed with Google verify -> 201.

    The PENDING order is created inside mock_commit (simulating a concurrent
    INSERT that beats ours), so the initial SELECT returns None (race window)
    and the re-read after IntegrityError finds the winning PENDING order.
    """
    from unittest.mock import patch as mock_patch

    from sqlalchemy.exc import IntegrityError as SAIntegrityError

    auth = authenticated_client

    db_session.add(AppConfig(key="credit_products", value=CREDIT_PRODUCTS_JSON))
    await db_session.flush()

    # Do NOT pre-create the order — it must be absent for the initial SELECT
    original_flush = db_session.flush

    commit_call_count = {"n": 0}

    async def mock_commit():
        commit_call_count["n"] += 1
        if commit_call_count["n"] == 1:
            # Simulate: another process inserted an order before ours.
            # Remove the phantom order from our failed INSERT attempt.
            for obj in list(db_session.new):
                db_session.expunge(obj)
            # Create the "winning" order (will be found by the re-read)
            winning_order = PurchaseOrder(
                user_id=auth.user_id,
                product_id="credits_10",
                purchase_token="race-token-pending",
                credits_amount=10,
                status=PurchaseStatus.PENDING.value,
            )
            db_session.add(winning_order)
            await original_flush()
            raise SAIntegrityError("duplicate key", {}, None)
        await original_flush()

    async def mock_rollback():
        # winning_order was already flushed (persistent) — only expunge unflushed
        for obj in list(db_session.new):
            db_session.expunge(obj)

    mock_svc = _mock_gp_service()

    with (
        mock_patch.object(db_session, "commit", side_effect=mock_commit),
        mock_patch.object(db_session, "rollback", side_effect=mock_rollback),
        mock_patch("app.routers.credits._create_google_play_service", return_value=mock_svc),
    ):
        resp = await auth.client.post(
            PURCHASE_URL,
            json={
                "product_id": "credits_10",
                "purchase_token": "race-token-pending",
            },
            headers=auth.headers,
        )

    assert resp.status_code == 201
    data = resp.json()
    assert data["credits_added"] == 10
    assert data["new_balance"] == 20  # 10 bonus + 10 purchase

    # Google verify and consume were called (proceeded through full flow)
    mock_svc.verify_purchase.assert_called_once()
    mock_svc.consume_purchase.assert_called_once()


# ---------------------------------------------------------------------------
# Test 24: google_order_id IntegrityError on consume commit -> 200
# ---------------------------------------------------------------------------


async def test_google_order_id_race_on_consume_returns_200(
    authenticated_client, db_session, fake_redis
):
    """IntegrityError on CONSUMED commit (google_order_id conflict):
    another order already claimed this google_order_id -> 200.
    """
    from unittest.mock import AsyncMock
    from unittest.mock import patch as mock_patch

    from sqlalchemy.exc import IntegrityError as SAIntegrityError

    auth = authenticated_client

    db_session.add(AppConfig(key="credit_products", value=CREDIT_PRODUCTS_JSON))
    await db_session.flush()

    # Pre-create a VERIFIED order that already has this google_order_id
    existing_order = PurchaseOrder(
        user_id=auth.user_id,
        product_id="credits_50",
        purchase_token="first-token-order-id-race",
        credits_amount=50,
        status=PurchaseStatus.VERIFIED.value,
        google_order_id="GPA.shared-order-race",
        verified_at=datetime.now(UTC),
    )
    db_session.add(existing_order)

    balance_result = await db_session.execute(
        select(CreditBalance).where(CreditBalance.user_id == auth.user_id)
    )
    balance_row = balance_result.scalar_one()
    balance_row.balance = 60
    fake_redis._store[f"user_balance:{auth.user_id}"] = "60"
    await db_session.flush()

    mock_svc = _mock_gp_service(
        verify_result=GooglePurchaseResult(
            order_id="GPA.shared-order-race",
            purchase_state=0,
            consumption_state=0,
            acknowledgement_state=0,
            purchase_time_millis="1708700000000",
            already_consumed=False,
        )
    )

    commit_call_count = {"n": 0}
    original_commit = db_session.commit

    async def mock_commit():
        commit_call_count["n"] += 1
        if commit_call_count["n"] == 2:
            # Second commit (CONSUMED + google_order_id) — simulate race
            raise SAIntegrityError("duplicate key google_order_id", {}, None)
        return await original_commit()

    with (
        mock_patch.object(db_session, "commit", side_effect=mock_commit),
        mock_patch.object(db_session, "rollback", new_callable=AsyncMock),
        mock_patch("app.routers.credits._create_google_play_service", return_value=mock_svc),
    ):
        resp = await auth.client.post(
            PURCHASE_URL,
            json={
                "product_id": "credits_50",
                "purchase_token": "second-token-order-id-race",
            },
            headers=auth.headers,
        )

    assert resp.status_code == 200
    data = resp.json()
    assert data["credits_added"] == 50
    assert data["new_balance"] == 60  # No new credits — dedup
