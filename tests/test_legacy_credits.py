"""Tests for legacy credit claim flow via PATCH /profile."""

from sqlalchemy import select

from app.models.credit_transaction import CreditTransaction, TransactionType
from app.models.legacy_credit import LegacyCredit

PROFILE_URL = "/api/v1/profile"
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


# --- 1. Successful claim: phone + license match → credits transferred ---


async def test_legacy_credits_claimed_on_profile_update(app_client, db_session):
    """PATCH /profile with matching phone+license → legacy credits transferred."""
    await _seed_legacy(db_session)
    reg = await _register(app_client, "claim1@example.com")

    resp = await app_client.patch(
        PROFILE_URL,
        json={"phone_number": LEGACY_PHONE, "license_number": LEGACY_LICENSE},
        headers=_auth(reg["access_token"]),
    )

    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["legacy_credits_claimed"] == LEGACY_BALANCE


async def test_legacy_claim_creates_transaction(app_client, db_session):
    """Successful claim creates a LEGACY_IMPORT CreditTransaction."""
    await _seed_legacy(db_session)
    reg = await _register(app_client, "claim2@example.com")

    await app_client.patch(
        PROFILE_URL,
        json={"phone_number": LEGACY_PHONE, "license_number": LEGACY_LICENSE},
        headers=_auth(reg["access_token"]),
    )

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


async def test_legacy_claim_zeroes_legacy_balance(app_client, db_session):
    """After claim, legacy_credits.balance is set to 0 and claimed_at is set."""
    legacy = await _seed_legacy(db_session)
    reg = await _register(app_client, "claim3@example.com")

    await app_client.patch(
        PROFILE_URL,
        json={"phone_number": LEGACY_PHONE, "license_number": LEGACY_LICENSE},
        headers=_auth(reg["access_token"]),
    )

    await db_session.refresh(legacy)
    assert legacy.balance == 0
    assert legacy.claimed_at is not None


# --- 2. Already claimed → no second transfer ---


async def test_already_claimed_user_gets_null(app_client, db_session):
    """Second PATCH /profile after successful claim → legacy_credits_claimed=null."""
    await _seed_legacy(db_session)
    reg = await _register(app_client, "claimed1@example.com")
    headers = _auth(reg["access_token"])

    # First claim
    resp1 = await app_client.patch(
        PROFILE_URL,
        json={"phone_number": LEGACY_PHONE, "license_number": LEGACY_LICENSE},
        headers=headers,
    )
    assert resp1.json()["legacy_credits_claimed"] == LEGACY_BALANCE

    # Second attempt — already claimed
    resp2 = await app_client.patch(
        PROFILE_URL,
        json={"license_number": LEGACY_LICENSE},
        headers=headers,
    )
    assert resp2.json()["legacy_credits_claimed"] is None


# --- 3. No match → null ---


async def test_no_legacy_match_returns_null(app_client, db_session):
    """PATCH /profile with non-matching phone+license → legacy_credits_claimed=null."""
    await _seed_legacy(db_session)
    reg = await _register(app_client, "nomatch@example.com")

    resp = await app_client.patch(
        PROFILE_URL,
        json={"phone_number": "+19999999999", "license_number": "WRONG123"},
        headers=_auth(reg["access_token"]),
    )

    assert resp.status_code == 200
    assert resp.json()["legacy_credits_claimed"] is None


# --- 4. Missing field → no lookup ---


async def test_only_phone_no_license_skips_claim(app_client, db_session):
    """PATCH /profile with only phone_number (no license) → no claim attempt."""
    await _seed_legacy(db_session)
    reg = await _register(app_client, "phoneonly@example.com")

    resp = await app_client.patch(
        PROFILE_URL,
        json={"phone_number": LEGACY_PHONE},
        headers=_auth(reg["access_token"]),
    )

    assert resp.status_code == 200
    assert resp.json()["legacy_credits_claimed"] is None


# --- 5. Zero balance legacy record → null ---


async def test_zero_balance_legacy_returns_null(app_client, db_session):
    """Legacy record with balance=0 → no transfer."""
    await _seed_legacy(db_session, balance=0)
    reg = await _register(app_client, "zero@example.com")

    resp = await app_client.patch(
        PROFILE_URL,
        json={"phone_number": LEGACY_PHONE, "license_number": LEGACY_LICENSE},
        headers=_auth(reg["access_token"]),
    )

    assert resp.status_code == 200
    assert resp.json()["legacy_credits_claimed"] is None


# --- 6. Rate limiting ---


async def test_rate_limit_blocks_after_3_attempts(app_client, db_session, fake_redis):
    """After 3 failed attempts, further claim checks are skipped."""
    await _seed_legacy(db_session)
    reg = await _register(app_client, "ratelimit@example.com")
    headers = _auth(reg["access_token"])

    # 3 failed attempts with wrong license
    for i in range(3):
        await app_client.patch(
            PROFILE_URL,
            json={"phone_number": LEGACY_PHONE, "license_number": f"WRONG{i}"},
            headers=headers,
        )

    # 4th attempt with correct data — should be rate limited
    resp = await app_client.patch(
        PROFILE_URL,
        json={"phone_number": LEGACY_PHONE, "license_number": LEGACY_LICENSE},
        headers=headers,
    )

    assert resp.status_code == 200
    assert resp.json()["legacy_credits_claimed"] is None


# --- 7. GET /auth/me reflects legacy_credits_claimed ---


async def test_me_shows_legacy_credits_claimed_false(app_client, db_session):
    """GET /auth/me → legacy_credits_claimed=false before any claim."""
    reg = await _register(app_client, "me_false@example.com")

    resp = await app_client.get(ME_URL, headers=_auth(reg["access_token"]))

    assert resp.status_code == 200
    assert resp.json()["legacy_credits_claimed"] is False


async def test_me_shows_legacy_credits_claimed_true_after_claim(app_client, db_session):
    """GET /auth/me → legacy_credits_claimed=true after successful claim."""
    await _seed_legacy(db_session)
    reg = await _register(app_client, "me_true@example.com")
    headers = _auth(reg["access_token"])

    await app_client.patch(
        PROFILE_URL,
        json={"phone_number": LEGACY_PHONE, "license_number": LEGACY_LICENSE},
        headers=headers,
    )

    resp = await app_client.get(ME_URL, headers=headers)

    assert resp.status_code == 200
    assert resp.json()["legacy_credits_claimed"] is True
