"""Tests for legacy credit restore flow via POST /credits/restore."""

from sqlalchemy import select

from app.models.credit_transaction import CreditTransaction, TransactionType
from app.models.legacy_credit import LegacyCredit

RESTORE_URL = "/api/v1/credits/restore"
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


async def _set_profile(app_client, token, phone=LEGACY_PHONE, license_num=LEGACY_LICENSE):
    """Set phone and license on user profile."""
    resp = await app_client.patch(
        PROFILE_URL,
        json={"phone_number": phone, "license_number": license_num},
        headers=_auth(token),
    )
    assert resp.status_code == 200


# --- 1. Successful restore: phone + license match → credits transferred ---


async def test_restore_credits_success(app_client, db_session):
    """POST /credits/restore with matching phone+license → 200, credits restored."""
    await _seed_legacy(db_session)
    reg = await _register(app_client, "restore1@example.com")
    await _set_profile(app_client, reg["access_token"])

    resp = await app_client.post(RESTORE_URL, headers=_auth(reg["access_token"]))

    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["restored_credits"] == LEGACY_BALANCE


async def test_restore_creates_transaction(app_client, db_session):
    """Successful restore creates a LEGACY_IMPORT CreditTransaction."""
    await _seed_legacy(db_session)
    reg = await _register(app_client, "restore2@example.com")
    await _set_profile(app_client, reg["access_token"])

    await app_client.post(RESTORE_URL, headers=_auth(reg["access_token"]))

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
    await _set_profile(app_client, reg["access_token"])

    await app_client.post(RESTORE_URL, headers=_auth(reg["access_token"]))

    await db_session.refresh(legacy)
    assert legacy.balance == 0
    assert legacy.claimed_at is not None


# --- 2. Already restored → 409 ---


async def test_already_restored_returns_409(app_client, db_session):
    """Second POST /credits/restore after successful restore → 409 ALREADY_RESTORED."""
    await _seed_legacy(db_session)
    reg = await _register(app_client, "restored1@example.com")
    await _set_profile(app_client, reg["access_token"])
    headers = _auth(reg["access_token"])

    resp1 = await app_client.post(RESTORE_URL, headers=headers)
    assert resp1.status_code == 200
    assert resp1.json()["restored_credits"] == LEGACY_BALANCE

    resp2 = await app_client.post(RESTORE_URL, headers=headers)
    assert resp2.status_code == 409


# --- 3. No match → 404 ---


async def test_no_legacy_match_returns_404(app_client, db_session):
    """POST /credits/restore with non-matching phone+license → 404 NO_MATCH."""
    await _seed_legacy(db_session)
    reg = await _register(app_client, "nomatch@example.com")
    await _set_profile(
        app_client, reg["access_token"], phone="+19999999999", license_num="WRONG123"
    )

    resp = await app_client.post(RESTORE_URL, headers=_auth(reg["access_token"]))

    assert resp.status_code == 404


# --- 4. Missing profile fields → 422 ---


async def test_incomplete_profile_returns_422(app_client, db_session):
    """POST /credits/restore without phone+license set → 422 INCOMPLETE_PROFILE."""
    await _seed_legacy(db_session)
    reg = await _register(app_client, "incomplete@example.com")

    resp = await app_client.post(RESTORE_URL, headers=_auth(reg["access_token"]))

    assert resp.status_code == 422


async def test_only_phone_no_license_returns_422(app_client, db_session):
    """POST /credits/restore with only phone (no license) → 422."""
    await _seed_legacy(db_session)
    reg = await _register(app_client, "phoneonly@example.com")
    await app_client.patch(
        PROFILE_URL,
        json={"phone_number": LEGACY_PHONE},
        headers=_auth(reg["access_token"]),
    )

    resp = await app_client.post(RESTORE_URL, headers=_auth(reg["access_token"]))

    assert resp.status_code == 422


# --- 5. Zero balance legacy record → 404 ---


async def test_zero_balance_legacy_returns_404(app_client, db_session):
    """Legacy record with balance=0 → 404 NO_MATCH."""
    await _seed_legacy(db_session, balance=0)
    reg = await _register(app_client, "zero@example.com")
    await _set_profile(app_client, reg["access_token"])

    resp = await app_client.post(RESTORE_URL, headers=_auth(reg["access_token"]))

    assert resp.status_code == 404


# --- 6. Rate limiting ---


async def test_rate_limit_blocks_after_3_attempts(app_client, db_session, fake_redis):
    """After 3 failed attempts, further restore attempts return 429 with retry_after_seconds."""
    await _seed_legacy(db_session)
    reg = await _register(app_client, "ratelimit@example.com")
    headers = _auth(reg["access_token"])

    # 3 failed attempts with wrong license
    for i in range(3):
        await _set_profile(app_client, reg["access_token"], license_num=f"WRONG{i}")
        await app_client.post(RESTORE_URL, headers=headers)

    # 4th attempt with correct data — should be rate limited by service layer
    await _set_profile(app_client, reg["access_token"])
    resp = await app_client.post(RESTORE_URL, headers=headers)

    # The service-level rate limit returns RATE_LIMITED → 429
    # (or the HTTP-level 3/hour limiter may trigger first — either way it's 429)
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
    await _set_profile(app_client, reg["access_token"])

    await app_client.post(RESTORE_URL, headers=_auth(reg["access_token"]))

    resp = await app_client.get(ME_URL, headers=_auth(reg["access_token"]))

    assert resp.status_code == 200
    assert resp.json()["legacy_credits_restored"] is True


# --- 8. PATCH /profile no longer triggers credit restore ---


async def test_profile_update_does_not_restore_credits(app_client, db_session):
    """PATCH /profile with matching phone+license does NOT auto-restore credits."""
    await _seed_legacy(db_session)
    reg = await _register(app_client, "norestore@example.com")

    resp = await app_client.patch(
        PROFILE_URL,
        json={"phone_number": LEGACY_PHONE, "license_number": LEGACY_LICENSE},
        headers=_auth(reg["access_token"]),
    )

    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert "legacy_credits_claimed" not in data
    assert "restored_credits" not in data

    # Verify no transaction was created
    result = await db_session.execute(
        select(CreditTransaction).where(
            CreditTransaction.user_id == reg["user_id"],
            CreditTransaction.type == TransactionType.LEGACY_IMPORT,
        )
    )
    assert result.scalar_one_or_none() is None
