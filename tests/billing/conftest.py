"""Billing-specific pytest fixtures for E2E tests.

These fixtures build on top of the root conftest.py (db_session, fake_redis,
app_client, authenticated_client, device_headers) and provide billing-specific
setup: AppConfig seeding, Google Play API mocking, users with specific balances.
"""

import json
from unittest.mock import AsyncMock, MagicMock

import pytest_asyncio

from app.models.app_config import AppConfig
from app.models.credit_balance import CreditBalance
from app.models.credit_transaction import CreditTransaction, TransactionType
from app.models.search_filters import SearchFilters
from app.models.search_status import SearchStatus
from app.models.user import User
from app.services.google_play_service import GooglePurchaseResult
from tests.factories import UserFactory

# ---------------------------------------------------------------------------
# Default test values matching PRD section 4 (product catalog)
# ---------------------------------------------------------------------------

DEFAULT_CREDIT_PRODUCTS = [
    {"product_id": "credits_10", "credits": 10, "price_usd": 10.00},
    {"product_id": "credits_25", "credits": 25, "price_usd": 22.00},
    {"product_id": "credits_50", "credits": 50, "price_usd": 40.00},
    {"product_id": "credits_100", "credits": 100, "price_usd": 80.00},
]

DEFAULT_RIDE_CREDIT_TIERS = [
    {"max_price": 20.0, "credits": 1},
    {"max_price": 50.0, "credits": 2},
    {"max_price": None, "credits": 3},
]

DEFAULT_REGISTRATION_BONUS = 10
DEFAULT_VERIFICATION_DEADLINE_MINUTES = 5  # shorter for tests
DEFAULT_VERIFICATION_CHECK_INTERVAL_MINUTES = 60


# ---------------------------------------------------------------------------
# billing_app_config — seeds all billing-related AppConfig entries
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def billing_app_config(db_session):
    """Seed all billing-related AppConfig keys for E2E tests.

    Values match PRD defaults. verification_deadline_minutes is shortened
    to 5 minutes for faster test execution.

    Returns a dict of {key: value} for introspection in tests.
    """
    configs = {
        "credit_products": json.dumps(DEFAULT_CREDIT_PRODUCTS),
        "ride_credit_tiers": json.dumps(DEFAULT_RIDE_CREDIT_TIERS),
        "registration_bonus_credits": str(DEFAULT_REGISTRATION_BONUS),
        "verification_deadline_minutes": str(DEFAULT_VERIFICATION_DEADLINE_MINUTES),
        "verification_check_interval_minutes": str(DEFAULT_VERIFICATION_CHECK_INTERVAL_MINUTES),
    }

    for key, value in configs.items():
        db_session.add(AppConfig(key=key, value=value))
    await db_session.flush()

    return configs


# ---------------------------------------------------------------------------
# mock_google_play_service — configurable mock for GooglePlayService
# ---------------------------------------------------------------------------


def make_google_play_mock(
    *,
    verify_result: GooglePurchaseResult | None = None,
    verify_error: Exception | None = None,
    consume_result: bool = True,
) -> MagicMock:
    """Create a mock GooglePlayService with configurable behavior.

    Args:
        verify_result: Custom result for verify_purchase(). Defaults to a
            valid purchase with order_id "GPA.test-order-001".
        verify_error: If set, verify_purchase() raises this exception.
        consume_result: Return value for consume_purchase(). Default True.

    Returns:
        MagicMock with async verify_purchase() and consume_purchase() methods.
    """
    svc = MagicMock()
    if verify_error:
        svc.verify_purchase = AsyncMock(side_effect=verify_error)
    else:
        svc.verify_purchase = AsyncMock(
            return_value=verify_result
            or GooglePurchaseResult(
                order_id="GPA.test-order-001",
                purchase_state=0,
                consumption_state=0,
                acknowledgement_state=0,
                purchase_time_millis="1708700000000",
                already_consumed=False,
            )
        )
    svc.consume_purchase = AsyncMock(return_value=consume_result)
    return svc


@pytest_asyncio.fixture
async def mock_google_play_service():
    """Provide a default mock GooglePlayService for purchase verification tests.

    Returns a MagicMock with verify_purchase() and consume_purchase() methods.
    Use make_google_play_mock() directly for custom configurations.
    """
    return make_google_play_mock()


# ---------------------------------------------------------------------------
# User fixtures with specific credit balances
# ---------------------------------------------------------------------------


async def _create_user_with_balance(db_session, *, email: str, balance: int) -> User:
    """Create a user with associated CreditBalance and REGISTRATION_BONUS transaction.

    Creates the full set of required related objects (SearchFilters, SearchStatus)
    to match the registration flow.
    """
    user = UserFactory.build(email=email)
    db_session.add(user)
    await db_session.flush()

    db_session.add(SearchFilters(user_id=user.id))
    db_session.add(SearchStatus(user_id=user.id))
    db_session.add(CreditBalance(user_id=user.id, balance=balance))
    db_session.add(
        CreditTransaction(
            user_id=user.id,
            type=TransactionType.REGISTRATION_BONUS,
            amount=balance,
            balance_after=balance,
        )
    )
    await db_session.flush()

    return user


@pytest_asyncio.fixture
async def user_with_balance(db_session):
    """Test user with CreditBalance(balance=10) and REGISTRATION_BONUS transaction.

    Returns the User ORM object. Associated CreditBalance, SearchFilters,
    SearchStatus, and CreditTransaction are created in the same DB session.
    """
    return await _create_user_with_balance(
        db_session, email="billing-user@example.com", balance=10
    )


@pytest_asyncio.fixture
async def user_zero_balance(db_session):
    """Test user with CreditBalance(balance=0).

    Simulates a user who has exhausted all credits. Returns the User ORM object.
    """
    return await _create_user_with_balance(db_session, email="zero-balance@example.com", balance=0)
