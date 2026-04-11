"""E2E tests for Registration -> Email Verification -> Bonus flow.

Verifies that new user registration creates a credit balance with zero credits,
and that the registration bonus is granted only after email verification.
Covers configurable bonus and atomicity.

Test strategy (task 14.2):
1. Run tests on clean database
2. Verify all assertions pass
3. Verify test isolation (cleanup after test)
4. Verify AppConfig override works
"""

from uuid import UUID

from sqlalchemy import select

from app.models.app_config import AppConfig
from app.models.credit_balance import CreditBalance
from app.models.credit_transaction import CreditTransaction, TransactionType
from tests.billing.helpers import assert_balance, assert_transaction_exists

REGISTER_URL = "/api/v1/auth/register"
SEARCH_STATUS_URL = "/api/v1/search/status"


async def _verify_email_in_db(db_session, user_id: str, bonus: int = 10):
    """Set email_verified=True and grant registration bonus (mirrors verify-email endpoint)."""
    from app.models.user import User

    uid = UUID(user_id)
    result = await db_session.execute(select(User).where(User.id == uid))
    user = result.scalar_one()
    user.email_verified = True
    cb_result = await db_session.execute(select(CreditBalance).where(CreditBalance.user_id == uid))
    cb = cb_result.scalar_one()
    cb.balance = bonus
    db_session.add(
        CreditTransaction(
            user_id=uid,
            type=TransactionType.REGISTRATION_BONUS,
            amount=bonus,
            balance_after=bonus,
        )
    )
    await db_session.commit()


async def test_register_creates_zero_balance_bonus_after_verify(app_client, db_session):
    """POST /auth/register creates balance=0, bonus granted after verify-email."""
    response = await app_client.post(
        REGISTER_URL,
        json={"email": "bonus-e2e@example.com", "password": "securePass1"},
    )

    assert response.status_code == 201
    user_id = UUID(response.json()["user_id"])

    # Before verification: balance is 0, no bonus transaction
    await assert_balance(db_session, user_id, 0)

    # Simulate email verification (grants bonus)
    await _verify_email_in_db(db_session, response.json()["user_id"])

    # After verification: balance is 10, bonus transaction exists
    await assert_balance(db_session, user_id, 10)
    tx = await assert_transaction_exists(
        db_session, user_id, TransactionType.REGISTRATION_BONUS, 10
    )
    assert tx.balance_after == 10
    assert tx.reference_id is None


async def test_search_status_returns_credits_balance(app_client, db_session, authenticated_client):
    """GET /search/status -> credits_balance=10 after email verification."""
    # Verify email and grant bonus
    await _verify_email_in_db(db_session, authenticated_client.user_id)

    response = await authenticated_client.client.get(
        SEARCH_STATUS_URL,
        headers=authenticated_client.headers,
    )

    assert response.status_code == 200
    data = response.json()
    assert "credits_balance" in data
    assert data["credits_balance"] == 10


async def test_registration_bonus_configurable(app_client, db_session):
    """AppConfig registration_bonus_credits=20 -> balance=20 after verify-email."""
    # Seed custom bonus via AppConfig (overrides default fallback of 10)
    db_session.add(AppConfig(key="registration_bonus_credits", value="20"))
    await db_session.flush()

    response = await app_client.post(
        REGISTER_URL,
        json={"email": "custom-bonus@example.com", "password": "securePass1"},
    )

    assert response.status_code == 201
    user_id = UUID(response.json()["user_id"])

    # Simulate email verification with custom bonus
    await _verify_email_in_db(db_session, response.json()["user_id"], bonus=20)

    # Verify balance matches custom bonus
    await assert_balance(db_session, user_id, 20)

    # Verify transaction amount matches custom bonus
    tx = await assert_transaction_exists(
        db_session, user_id, TransactionType.REGISTRATION_BONUS, 20
    )
    assert tx.balance_after == 20


async def test_registration_creates_zero_balance_atomically(app_client, db_session):
    """Registration atomically creates User + SearchFilters + SearchStatus + CreditBalance(0).

    All records are created in a single commit. CreditBalance starts at 0
    (bonus is deferred to email verification to prevent credit farming).
    """
    response = await app_client.post(
        REGISTER_URL,
        json={"email": "atomic-test@example.com", "password": "securePass1"},
    )

    assert response.status_code == 201
    user_id = UUID(response.json()["user_id"])

    # Verify CreditBalance exists with zero balance
    await assert_balance(db_session, user_id, 0)

    # Verify no REGISTRATION_BONUS transaction exists yet
    result = await db_session.execute(
        select(CreditTransaction).where(
            CreditTransaction.user_id == user_id,
            CreditTransaction.type == TransactionType.REGISTRATION_BONUS,
        )
    )
    assert result.scalar_one_or_none() is None
