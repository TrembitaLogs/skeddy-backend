from unittest.mock import AsyncMock, patch
from uuid import UUID

import pytest
from sqlalchemy import select

from app.models.credit_balance import CreditBalance
from app.models.credit_transaction import CreditTransaction, TransactionType
from app.models.refresh_token import RefreshToken
from app.models.search_filters import SearchFilters
from app.models.search_status import SearchStatus
from app.models.user import User
from app.services.auth_service import decode_access_token, hash_refresh_token

REGISTER_URL = "/api/v1/auth/register"


async def test_register_valid_data_returns_201_with_tokens(app_client):
    """POST /auth/register with valid data -> 201 with tokens."""
    response = await app_client.post(
        REGISTER_URL,
        json={"email": "new@example.com", "password": "securePass1"},
    )

    assert response.status_code == 201
    data = response.json()
    assert "user_id" in data
    UUID(data["user_id"])  # must be valid UUID
    assert "access_token" in data
    assert "refresh_token" in data

    # Verify access token is a valid JWT with correct user_id
    payload = decode_access_token(data["access_token"])
    assert payload is not None
    assert payload["sub"] == data["user_id"]


async def test_register_duplicate_email_returns_409(app_client, db_session):
    """POST /auth/register with existing email -> 409 EMAIL_ALREADY_EXISTS."""
    # Create an existing user directly in the DB
    user = User(email="taken@example.com", password_hash="fakehash")
    db_session.add(user)
    await db_session.flush()

    response = await app_client.post(
        REGISTER_URL,
        json={"email": "taken@example.com", "password": "securePass1"},
    )

    assert response.status_code == 409
    assert response.json()["error"]["code"] == "EMAIL_ALREADY_EXISTS"


async def test_register_creates_user_search_filters_search_status(app_client, db_session):
    """POST /auth/register creates User, SearchFilters, SearchStatus in DB."""
    response = await app_client.post(
        REGISTER_URL,
        json={"email": "full@example.com", "password": "securePass1"},
    )

    assert response.status_code == 201
    user_id = UUID(response.json()["user_id"])

    # Verify User exists
    result = await db_session.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()
    assert user is not None
    assert user.email == "full@example.com"

    # Verify SearchFilters created with defaults
    result = await db_session.execute(
        select(SearchFilters).where(SearchFilters.user_id == user_id)
    )
    sf = result.scalar_one_or_none()
    assert sf is not None
    assert sf.min_price == 20.0
    assert sf.start_time == "06:30"
    assert sf.working_time == 24
    assert sf.working_days == ["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"]

    # Verify SearchStatus created with defaults
    result = await db_session.execute(select(SearchStatus).where(SearchStatus.user_id == user_id))
    ss = result.scalar_one_or_none()
    assert ss is not None
    assert ss.is_active is False


async def test_register_short_password_returns_422(app_client):
    """POST /auth/register with password < 8 chars -> 422 Validation Error."""
    response = await app_client.post(
        REGISTER_URL,
        json={"email": "short@example.com", "password": "short"},
    )

    assert response.status_code == 422


async def test_register_refresh_token_stored_hashed(app_client, db_session):
    """Refresh token is stored as SHA256 hash in DB, not plain text."""
    response = await app_client.post(
        REGISTER_URL,
        json={"email": "hash@example.com", "password": "securePass1"},
    )

    assert response.status_code == 201
    data = response.json()
    user_id = UUID(data["user_id"])
    refresh_token = data["refresh_token"]

    # Query the RefreshToken record
    result = await db_session.execute(select(RefreshToken).where(RefreshToken.user_id == user_id))
    rt = result.scalar_one_or_none()
    assert rt is not None

    # Verify stored hash matches SHA256 of the returned token
    expected_hash = hash_refresh_token(refresh_token)
    assert rt.token_hash == expected_hash

    # Verify the plain token is NOT stored
    assert rt.token_hash != refresh_token


# ---------------------------------------------------------------------------
# Billing: CreditBalance created with registration bonus
# ---------------------------------------------------------------------------


async def test_register_creates_credit_balance_with_bonus(app_client, db_session):
    """POST /auth/register creates CreditBalance with default bonus of 10 credits."""
    response = await app_client.post(
        REGISTER_URL,
        json={"email": "credit-bal@example.com", "password": "securePass1"},
    )

    assert response.status_code == 201
    user_id = UUID(response.json()["user_id"])

    result = await db_session.execute(
        select(CreditBalance).where(CreditBalance.user_id == user_id)
    )
    cb = result.scalar_one_or_none()
    assert cb is not None
    assert cb.balance == 10


# ---------------------------------------------------------------------------
# Billing: CreditTransaction REGISTRATION_BONUS created
# ---------------------------------------------------------------------------


async def test_register_creates_registration_bonus_transaction(app_client, db_session):
    """POST /auth/register creates REGISTRATION_BONUS CreditTransaction with correct fields."""
    response = await app_client.post(
        REGISTER_URL,
        json={"email": "bonus-tx@example.com", "password": "securePass1"},
    )

    assert response.status_code == 201
    user_id = UUID(response.json()["user_id"])

    result = await db_session.execute(
        select(CreditTransaction).where(CreditTransaction.user_id == user_id)
    )
    tx = result.scalar_one_or_none()
    assert tx is not None
    assert tx.type == TransactionType.REGISTRATION_BONUS
    assert tx.amount == 10
    assert tx.balance_after == 10
    assert tx.reference_id is None


# ---------------------------------------------------------------------------
# Billing: Atomicity — credit creation failure prevents user creation
# ---------------------------------------------------------------------------


async def test_register_fails_when_credit_creation_fails(app_client):
    """Register fails when create_balance_with_bonus raises.

    The register endpoint uses a single commit() for User + SearchFilters +
    SearchStatus + CreditBalance + CreditTransaction. If create_balance_with_bonus
    raises before commit(), the exception propagates and nothing is persisted.
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
            json={"email": "rollback@example.com", "password": "securePass1"},
        )


# ---------------------------------------------------------------------------
# Billing: Response format unchanged (no billing fields leaked)
# ---------------------------------------------------------------------------


async def test_register_response_has_no_billing_fields(app_client):
    """POST /auth/register response contains only auth fields, no billing data."""
    response = await app_client.post(
        REGISTER_URL,
        json={"email": "format@example.com", "password": "securePass1"},
    )

    assert response.status_code == 201
    data = response.json()
    assert set(data.keys()) == {"user_id", "access_token", "refresh_token"}
