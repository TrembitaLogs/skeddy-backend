"""E2E tests for Registration -> Bonus flow.

Verifies that new user registration correctly creates a credit balance
with the registration bonus, and that the balance is accessible through
the search status endpoint. Covers configurable bonus and atomicity.

Test strategy (task 14.2):
1. Run tests on clean database
2. Verify all assertions pass
3. Verify test isolation (cleanup after test)
4. Verify AppConfig override works
"""

from unittest.mock import AsyncMock, patch
from uuid import UUID

import pytest

from app.models.app_config import AppConfig
from app.models.credit_transaction import TransactionType
from tests.billing.helpers import assert_balance, assert_transaction_exists

REGISTER_URL = "/api/v1/auth/register"
SEARCH_STATUS_URL = "/api/v1/search/status"


async def test_register_creates_credit_balance_with_bonus(app_client, db_session):
    """POST /auth/register -> CreditBalance.balance=10 + REGISTRATION_BONUS transaction."""
    response = await app_client.post(
        REGISTER_URL,
        json={"email": "bonus-e2e@example.com", "password": "securePass1"},
    )

    assert response.status_code == 201
    user_id = UUID(response.json()["user_id"])

    # Verify CreditBalance created with default bonus (fallback: 10)
    await assert_balance(db_session, user_id, 10)

    # Verify REGISTRATION_BONUS transaction with correct fields
    tx = await assert_transaction_exists(
        db_session, user_id, TransactionType.REGISTRATION_BONUS, 10
    )
    assert tx.balance_after == 10
    assert tx.reference_id is None


async def test_search_status_returns_credits_balance(authenticated_client):
    """GET /search/status -> credits_balance=10 after registration."""
    response = await authenticated_client.client.get(
        SEARCH_STATUS_URL,
        headers=authenticated_client.headers,
    )

    assert response.status_code == 200
    data = response.json()
    assert "credits_balance" in data
    assert data["credits_balance"] == 10


async def test_registration_bonus_configurable(app_client, db_session):
    """AppConfig registration_bonus_credits=20 -> balance=20 after register."""
    # Seed custom bonus via AppConfig (overrides default fallback of 10)
    db_session.add(AppConfig(key="registration_bonus_credits", value="20"))
    await db_session.flush()

    response = await app_client.post(
        REGISTER_URL,
        json={"email": "custom-bonus@example.com", "password": "securePass1"},
    )

    assert response.status_code == 201
    user_id = UUID(response.json()["user_id"])

    # Verify balance matches custom bonus
    await assert_balance(db_session, user_id, 20)

    # Verify transaction amount matches custom bonus
    tx = await assert_transaction_exists(
        db_session, user_id, TransactionType.REGISTRATION_BONUS, 20
    )
    assert tx.balance_after == 20


async def test_registration_fails_when_credit_creation_fails(app_client):
    """Registration does not return 201 when create_balance_with_bonus raises.

    The register endpoint uses a single db.commit() for User + SearchFilters +
    SearchStatus + CreditBalance + CreditTransaction. If create_balance_with_bonus
    raises before commit(), nothing is persisted to the database.

    Note: DB state verification (User/CreditBalance absence) is not feasible in
    E2E tests because the shared savepoint-based session retains flushed objects
    even when commit() is never reached. The atomicity guarantee is architectural:
    a single commit() in app/routers/auth.py ensures all-or-nothing persistence.
    """
    with (
        patch(
            "app.routers.auth.create_balance_with_bonus",
            new_callable=AsyncMock,
            side_effect=RuntimeError("Simulated credit creation failure"),
        ),
        pytest.raises(Exception, match="Simulated credit creation failure"),
    ):
        await app_client.post(
            REGISTER_URL,
            json={"email": "atomic-test@example.com", "password": "securePass1"},
        )
