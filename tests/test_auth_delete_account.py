from uuid import UUID

from sqlalchemy import select

from app.models.accept_failure import AcceptFailure
from app.models.credit_balance import CreditBalance
from app.models.credit_transaction import CreditTransaction, TransactionType
from app.models.paired_device import PairedDevice
from app.models.purchase_order import PurchaseOrder
from app.models.refresh_token import RefreshToken
from app.models.ride import Ride
from app.models.search_filters import SearchFilters
from app.models.search_status import SearchStatus
from app.models.user import User

DELETE_ACCOUNT_URL = "/api/v1/auth/account"
REGISTER_URL = "/api/v1/auth/register"
LOGIN_URL = "/api/v1/auth/login"

_TEST_PASSWORD = "securePass1"


async def _register_and_get_tokens(app_client, email="delete@example.com"):
    """Helper: register a user via API and return response data with tokens."""
    response = await app_client.post(
        REGISTER_URL,
        json={"email": email, "password": _TEST_PASSWORD},
    )
    assert response.status_code == 201
    return response.json()


def _auth_header(access_token: str) -> dict:
    return {"Authorization": f"Bearer {access_token}"}


# --- Test Strategy: 1. DELETE /auth/account with correct password → 200, user deleted ---


async def test_delete_account_valid_password_returns_200(app_client):
    """DELETE /auth/account with correct password → 200 {"ok": true}."""
    reg = await _register_and_get_tokens(app_client, email="del-ok@example.com")

    response = await app_client.request(
        "DELETE",
        DELETE_ACCOUNT_URL,
        json={"password": _TEST_PASSWORD},
        headers=_auth_header(reg["access_token"]),
    )

    assert response.status_code == 200
    assert response.json() == {"ok": True}


# --- Test Strategy: 2. DELETE /auth/account with wrong password → 401 ---


async def test_delete_account_wrong_password_returns_401(app_client):
    """DELETE /auth/account with wrong password → 401 INVALID_CREDENTIALS."""
    reg = await _register_and_get_tokens(app_client, email="del-wrong@example.com")

    response = await app_client.request(
        "DELETE",
        DELETE_ACCOUNT_URL,
        json={"password": "wrongPassword123"},
        headers=_auth_header(reg["access_token"]),
    )

    assert response.status_code == 401
    assert response.json()["error"]["code"] == "INVALID_CREDENTIALS"


# --- Test Strategy: 3. DELETE /auth/account without auth → 401 ---


async def test_delete_account_without_jwt_returns_401(app_client):
    """DELETE /auth/account without Authorization header → 401."""
    response = await app_client.request(
        "DELETE",
        DELETE_ACCOUNT_URL,
        json={"password": _TEST_PASSWORD},
    )

    assert response.status_code == 401


# --- Test Strategy: 4. After deletion, user does not exist in DB ---


async def test_delete_account_removes_user_from_db(app_client, db_session):
    """After deletion, user row must not exist in the users table."""
    reg = await _register_and_get_tokens(app_client, email="del-db@example.com")
    user_id = reg["user_id"]

    response = await app_client.request(
        "DELETE",
        DELETE_ACCOUNT_URL,
        json={"password": _TEST_PASSWORD},
        headers=_auth_header(reg["access_token"]),
    )
    assert response.status_code == 200

    # Verify user is gone
    result = await db_session.execute(select(User).where(User.id == user_id))
    assert result.scalar_one_or_none() is None


# --- Additional: cascaded data is removed ---


async def test_delete_account_cascades_all_related_data(app_client, db_session):
    """After deletion, ALL related records across all 9 child tables are removed."""
    reg = await _register_and_get_tokens(app_client, email="del-cascade@example.com")
    user_id = UUID(reg["user_id"])

    # Registration auto-creates: RefreshToken, SearchFilters, SearchStatus,
    # CreditBalance (balance=0, bonus deferred to verify-email).
    # Manually create CreditTransaction and remaining related entities.
    db_session.add(
        CreditTransaction(
            user_id=user_id,
            type=TransactionType.REGISTRATION_BONUS,
            amount=10,
            balance_after=10,
        )
    )
    device = PairedDevice(
        user_id=user_id,
        device_id="cascade-test-device",
        device_token_hash="a" * 64,
        timezone="America/New_York",
    )
    db_session.add(device)

    ride = Ride(
        user_id=user_id,
        idempotency_key="cascade-test-ride-key",
        event_type="ACCEPTED",
        ride_data={"price": 30.0, "pickup_time": "09:00 AM"},
        ride_hash="a" * 64,
    )
    db_session.add(ride)

    failure = AcceptFailure(
        user_id=user_id,
        reason="TIMEOUT",
        ride_price=25.0,
    )
    db_session.add(failure)

    purchase = PurchaseOrder(
        user_id=user_id,
        product_id="credits_50",
        purchase_token="cascade-test-token",
        credits_amount=50,
    )
    db_session.add(purchase)
    await db_session.flush()

    # Confirm billing records exist before deletion
    bal = await db_session.execute(select(CreditBalance).where(CreditBalance.user_id == user_id))
    assert bal.scalar_one_or_none() is not None

    txns = await db_session.execute(
        select(CreditTransaction).where(CreditTransaction.user_id == user_id)
    )
    assert len(txns.scalars().all()) >= 1

    response = await app_client.request(
        "DELETE",
        DELETE_ACCOUNT_URL,
        json={"password": _TEST_PASSWORD},
        headers=_auth_header(reg["access_token"]),
    )
    assert response.status_code == 200

    # Verify every child table is empty for this user_id
    tokens = await db_session.execute(select(RefreshToken).where(RefreshToken.user_id == user_id))
    assert tokens.scalars().all() == []

    filters = await db_session.execute(
        select(SearchFilters).where(SearchFilters.user_id == user_id)
    )
    assert filters.scalar_one_or_none() is None

    status = await db_session.execute(select(SearchStatus).where(SearchStatus.user_id == user_id))
    assert status.scalar_one_or_none() is None

    devices = await db_session.execute(select(PairedDevice).where(PairedDevice.user_id == user_id))
    assert devices.scalar_one_or_none() is None

    rides = await db_session.execute(select(Ride).where(Ride.user_id == user_id))
    assert rides.scalars().all() == []

    failures = await db_session.execute(
        select(AcceptFailure).where(AcceptFailure.user_id == user_id)
    )
    assert failures.scalars().all() == []

    balances = await db_session.execute(
        select(CreditBalance).where(CreditBalance.user_id == user_id)
    )
    assert balances.scalar_one_or_none() is None

    transactions = await db_session.execute(
        select(CreditTransaction).where(CreditTransaction.user_id == user_id)
    )
    assert transactions.scalars().all() == []

    orders = await db_session.execute(
        select(PurchaseOrder).where(PurchaseOrder.user_id == user_id)
    )
    assert orders.scalars().all() == []


# --- Additional: after deletion, login with same credentials fails ---


async def test_delete_account_prevents_subsequent_login(app_client):
    """After deletion, login with the same credentials must fail."""
    email = "del-login@example.com"
    reg = await _register_and_get_tokens(app_client, email=email)

    response = await app_client.request(
        "DELETE",
        DELETE_ACCOUNT_URL,
        json={"password": _TEST_PASSWORD},
        headers=_auth_header(reg["access_token"]),
    )
    assert response.status_code == 200

    login_response = await app_client.post(
        LOGIN_URL,
        json={"email": email, "password": _TEST_PASSWORD},
    )
    assert login_response.status_code == 401
