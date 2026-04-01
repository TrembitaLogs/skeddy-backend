"""Tests for legacy credit restore flow via POST /credits/restore."""

from uuid import UUID

from sqlalchemy import select

from app.models.credit_transaction import CreditTransaction, TransactionType
from app.models.legacy_credit import LegacyCredit
from app.models.user import User

RESTORE_URL = "/api/v1/credits/restore"
REGISTER_URL = "/api/v1/auth/register"
ME_URL = "/api/v1/auth/me"

_TEST_PASSWORD = "securePass1"

LEGACY_PHONE = "+17328615954"
LEGACY_LICENSE = "P07447690005586"
LEGACY_BALANCE = 231


async def _register(app_client, email):
    resp = await app_client.post(REGISTER_URL, json={"email": email, "password": _TEST_PASSWORD})
    assert resp.status_code == 201
    return resp.json()


def _auth(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def _restore_body(phone=LEGACY_PHONE, license_num=LEGACY_LICENSE):
    return {"phone_number": phone, "license_number": license_num}


async def _seed_legacy(
    db_session, phone=LEGACY_PHONE, license_num=LEGACY_LICENSE, balance=LEGACY_BALANCE
):
    """Insert a legacy_credits row for testing."""
    legacy = LegacyCredit(
        old_user_id=1546,
        phone_number=phone,
        license_number=license_num,
        name="Test Driver",
        email="test@old.com",
        balance=balance,
    )
    db_session.add(legacy)
    await db_session.flush()
    return legacy


# --- 1. Successful restore: phone + license match → credits transferred ---


async def test_restore_credits_success(app_client, db_session):
    """POST /credits/restore with matching phone+license → 200, credits restored."""
    await _seed_legacy(db_session)
    reg = await _register(app_client, "restore1@example.com")

    resp = await app_client.post(
        RESTORE_URL, json=_restore_body(), headers=_auth(reg["access_token"])
    )

    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["restored_credits"] == LEGACY_BALANCE


async def test_restore_creates_transaction(app_client, db_session):
    """Successful restore creates a LEGACY_IMPORT CreditTransaction."""
    await _seed_legacy(db_session)
    reg = await _register(app_client, "restore2@example.com")

    await app_client.post(RESTORE_URL, json=_restore_body(), headers=_auth(reg["access_token"]))

    result = await db_session.execute(
        select(CreditTransaction).where(
            CreditTransaction.user_id == reg["user_id"],
            CreditTransaction.type == TransactionType.LEGACY_IMPORT,
        )
    )
    txn = result.scalar_one()
    assert txn.amount == LEGACY_BALANCE
    assert txn.description is not None
    assert "1546" in txn.description


async def test_restore_zeroes_legacy_balance(app_client, db_session):
    """After restore, legacy_credits.balance is set to 0 and claimed_at is set."""
    legacy = await _seed_legacy(db_session)
    reg = await _register(app_client, "restore3@example.com")

    await app_client.post(RESTORE_URL, json=_restore_body(), headers=_auth(reg["access_token"]))

    await db_session.refresh(legacy)
    assert legacy.balance == 0
    assert legacy.claimed_at is not None


async def test_restore_links_legacy_user_id(app_client, db_session):
    """Successful restore sets user.legacy_user_id to old_user_id."""
    await _seed_legacy(db_session)
    reg = await _register(app_client, "link1@example.com")

    await app_client.post(RESTORE_URL, json=_restore_body(), headers=_auth(reg["access_token"]))

    result = await db_session.execute(select(User).where(User.id == UUID(reg["user_id"])))
    user = result.scalar_one()
    assert user.legacy_user_id == 1546


# --- 2. Already restored → 409 ---


async def test_already_restored_returns_409(app_client, db_session):
    """Second POST /credits/restore after successful restore → 409 ALREADY_RESTORED."""
    await _seed_legacy(db_session)
    reg = await _register(app_client, "restored1@example.com")
    headers = _auth(reg["access_token"])

    resp1 = await app_client.post(RESTORE_URL, json=_restore_body(), headers=headers)
    assert resp1.status_code == 200

    resp2 = await app_client.post(RESTORE_URL, json=_restore_body(), headers=headers)
    assert resp2.status_code == 409


# --- 3. No match → 404 ---


async def test_no_legacy_match_returns_404(app_client, db_session):
    """POST /credits/restore with non-matching phone+license → 404 NO_MATCH."""
    await _seed_legacy(db_session)
    reg = await _register(app_client, "nomatch@example.com")

    resp = await app_client.post(
        RESTORE_URL,
        json=_restore_body(phone="+19999999999", license_num="WRONG123"),
        headers=_auth(reg["access_token"]),
    )

    assert resp.status_code == 404


# --- 4. Zero balance → 200 with restored_credits=0, legacy_user_id linked ---


async def test_zero_balance_returns_200_with_zero(app_client, db_session):
    """Legacy record with balance=0 → 200, restored_credits=0, user linked."""
    await _seed_legacy(db_session, balance=0)
    reg = await _register(app_client, "zero@example.com")

    resp = await app_client.post(
        RESTORE_URL, json=_restore_body(), headers=_auth(reg["access_token"])
    )

    assert resp.status_code == 200
    assert resp.json()["restored_credits"] == 0

    # Verify legacy_user_id is still linked
    result = await db_session.execute(select(User).where(User.id == UUID(reg["user_id"])))
    user = result.scalar_one()
    assert user.legacy_user_id == 1546


# --- 5. Negative balance → 200 with restored_credits=0 ---


async def test_negative_balance_returns_zero(app_client, db_session):
    """Legacy record with negative balance → 200, restored_credits=0."""
    await _seed_legacy(db_session, balance=-50)
    reg = await _register(app_client, "negative@example.com")

    resp = await app_client.post(
        RESTORE_URL, json=_restore_body(), headers=_auth(reg["access_token"])
    )

    assert resp.status_code == 200
    assert resp.json()["restored_credits"] == 0


# --- 6. Rate limiting ---


async def test_rate_limit_blocks_after_3_attempts(app_client, db_session, fake_redis):
    """After 3 failed attempts, further restore attempts return 429."""
    await _seed_legacy(db_session)
    reg = await _register(app_client, "ratelimit@example.com")
    headers = _auth(reg["access_token"])

    # 3 failed attempts with wrong license
    for i in range(3):
        await app_client.post(
            RESTORE_URL,
            json=_restore_body(license_num=f"WRONG{i}"),
            headers=headers,
        )

    # 4th attempt with correct data — should be rate limited by service layer
    resp = await app_client.post(RESTORE_URL, json=_restore_body(), headers=headers)

    assert resp.status_code == 429


# --- 7. GET /auth/me reflects legacy_credits_restored ---


async def test_me_shows_legacy_credits_restored_false(app_client, db_session):
    """GET /auth/me → legacy_credits_restored=false before any restore."""
    reg = await _register(app_client, "me_false@example.com")

    resp = await app_client.get(ME_URL, headers=_auth(reg["access_token"]))

    assert resp.status_code == 200
    assert resp.json()["legacy_credits_restored"] is False


async def test_me_shows_legacy_credits_restored_true_after_restore(app_client, db_session):
    """GET /auth/me → legacy_credits_restored=true after successful restore."""
    await _seed_legacy(db_session)
    reg = await _register(app_client, "me_true@example.com")

    await app_client.post(RESTORE_URL, json=_restore_body(), headers=_auth(reg["access_token"]))

    resp = await app_client.get(ME_URL, headers=_auth(reg["access_token"]))

    assert resp.status_code == 200
    assert resp.json()["legacy_credits_restored"] is True


# --- 8. Validation ---


async def test_invalid_phone_format_returns_422(app_client, db_session):
    """POST /credits/restore with invalid phone → 422."""
    reg = await _register(app_client, "badphone@example.com")

    resp = await app_client.post(
        RESTORE_URL,
        json={"phone_number": "not-a-phone", "license_number": "DL123"},
        headers=_auth(reg["access_token"]),
    )

    assert resp.status_code == 422


async def test_empty_license_returns_422(app_client, db_session):
    """POST /credits/restore with empty license → 422."""
    reg = await _register(app_client, "badlicense@example.com")

    resp = await app_client.post(
        RESTORE_URL,
        json={"phone_number": "+12025551234", "license_number": "  "},
        headers=_auth(reg["access_token"]),
    )

    assert resp.status_code == 422
