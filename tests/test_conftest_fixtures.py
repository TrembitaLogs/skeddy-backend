"""Validation tests for conftest fixtures and factory-boy factories.

Ensures all shared test infrastructure works correctly:
- Factory .build() creates valid model instances
- authenticated_client fixture provides working JWT auth
- device_headers fixture provides working device auth
"""

from uuid import UUID

from sqlalchemy import select

from app.models.accept_failure import AcceptFailure
from app.models.paired_device import PairedDevice
from app.models.refresh_token import RefreshToken
from app.models.ride import Ride
from app.models.search_filters import SearchFilters
from app.models.search_status import SearchStatus
from app.models.user import User
from app.services.auth_service import verify_password

from .factories import (
    TEST_PASSWORD,
    AcceptFailureFactory,
    PairedDeviceFactory,
    RefreshTokenFactory,
    RideFactory,
    SearchFiltersFactory,
    SearchStatusFactory,
    UserFactory,
)

# ---------------------------------------------------------------------------
# Factory .build() tests — verify model instances are created correctly
# ---------------------------------------------------------------------------


class TestUserFactory:
    def test_build_creates_user_with_defaults(self):
        user = UserFactory.build()
        assert isinstance(user.id, UUID)
        assert "@example.com" in user.email
        assert user.password_hash is not None
        assert user.phone_number is None
        assert user.fcm_token is None

    def test_password_hash_is_verifiable(self):
        user = UserFactory.build()
        assert verify_password(TEST_PASSWORD, user.password_hash)

    def test_build_with_overrides(self):
        user = UserFactory.build(email="custom@test.com", phone_number="+1234567890")
        assert user.email == "custom@test.com"
        assert user.phone_number == "+1234567890"

    def test_sequential_emails_are_unique(self):
        users = [UserFactory.build() for _ in range(3)]
        emails = {u.email for u in users}
        assert len(emails) == 3


class TestSearchFiltersFactory:
    def test_build_creates_filters_with_prd_defaults(self):
        sf = SearchFiltersFactory.build()
        assert sf.min_price == 20.0
        assert sf.start_time == "06:30"
        assert sf.working_time == 24
        assert sf.working_days == ["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"]

    def test_build_with_custom_user_id(self):
        user = UserFactory.build()
        sf = SearchFiltersFactory.build(user_id=user.id)
        assert sf.user_id == user.id


class TestSearchStatusFactory:
    def test_build_creates_inactive_status(self):
        ss = SearchStatusFactory.build()
        assert ss.is_active is False

    def test_build_active(self):
        ss = SearchStatusFactory.build(is_active=True)
        assert ss.is_active is True


class TestPairedDeviceFactory:
    def test_build_creates_device(self):
        pd = PairedDeviceFactory.build()
        assert pd.device_id.startswith("test-device-")
        assert len(pd.device_token_hash) == 64  # SHA256 hex
        assert pd.timezone == "America/New_York"
        assert pd.offline_notified is False

    def test_sequential_device_ids_are_unique(self):
        devices = [PairedDeviceFactory.build() for _ in range(3)]
        ids = {d.device_id for d in devices}
        assert len(ids) == 3


class TestRideFactory:
    def test_build_creates_ride(self):
        ride = RideFactory.build()
        assert ride.event_type == "ACCEPTED"
        assert isinstance(ride.ride_data, dict)
        assert "price" in ride.ride_data
        assert len(ride.idempotency_key) == 36  # UUID format


class TestAcceptFailureFactory:
    def test_build_creates_failure(self):
        af = AcceptFailureFactory.build()
        assert af.reason == "AcceptButtonNotFound"
        assert af.ride_price == 25.50


class TestRefreshTokenFactory:
    def test_build_creates_token(self):
        rt = RefreshTokenFactory.build()
        assert len(rt.token_hash) == 64  # SHA256 hex
        assert rt.expires_at is not None


# ---------------------------------------------------------------------------
# Factory + DB persistence — verify factories work with async session
# ---------------------------------------------------------------------------


async def test_factory_user_persists_to_db(db_session):
    """Factory-built user can be added to async session and queried back."""
    user = UserFactory.build()
    db_session.add(user)
    await db_session.flush()

    result = await db_session.execute(select(User).where(User.id == user.id))
    loaded = result.scalar_one()
    assert loaded.email == user.email


async def test_factory_related_models_persist_with_fk(db_session):
    """Factory-built related models respect FK constraints."""
    user = UserFactory.build()
    db_session.add(user)
    await db_session.flush()

    sf = SearchFiltersFactory.build(user_id=user.id)
    ss = SearchStatusFactory.build(user_id=user.id)
    db_session.add(sf)
    db_session.add(ss)
    await db_session.flush()

    result = await db_session.execute(
        select(SearchFilters).where(SearchFilters.user_id == user.id)
    )
    assert result.scalar_one().min_price == 20.0

    result = await db_session.execute(select(SearchStatus).where(SearchStatus.user_id == user.id))
    assert result.scalar_one().is_active is False


async def test_factory_ride_and_failure_persist(db_session):
    """Ride and AcceptFailure factories persist correctly with FK."""
    user = UserFactory.build()
    db_session.add(user)
    await db_session.flush()

    ride = RideFactory.build(user_id=user.id)
    failure = AcceptFailureFactory.build(user_id=user.id)
    db_session.add(ride)
    db_session.add(failure)
    await db_session.flush()

    result = await db_session.execute(select(Ride).where(Ride.user_id == user.id))
    assert result.scalar_one().event_type == "ACCEPTED"

    result = await db_session.execute(
        select(AcceptFailure).where(AcceptFailure.user_id == user.id)
    )
    assert result.scalar_one().reason == "AcceptButtonNotFound"


async def test_factory_paired_device_persists(db_session):
    """PairedDevice factory persists correctly with FK."""
    user = UserFactory.build()
    db_session.add(user)
    await db_session.flush()

    device = PairedDeviceFactory.build(user_id=user.id)
    db_session.add(device)
    await db_session.flush()

    result = await db_session.execute(select(PairedDevice).where(PairedDevice.user_id == user.id))
    loaded = result.scalar_one()
    assert loaded.device_id == device.device_id
    assert loaded.timezone == "America/New_York"


async def test_factory_refresh_token_persists(db_session):
    """RefreshToken factory persists correctly with FK."""
    user = UserFactory.build()
    db_session.add(user)
    await db_session.flush()

    rt = RefreshTokenFactory.build(user_id=user.id)
    db_session.add(rt)
    await db_session.flush()

    result = await db_session.execute(select(RefreshToken).where(RefreshToken.user_id == user.id))
    assert result.scalar_one().token_hash == rt.token_hash


# ---------------------------------------------------------------------------
# Convenience fixture tests — authenticated_client & device_headers
# ---------------------------------------------------------------------------


async def test_authenticated_client_provides_valid_jwt(authenticated_client):
    """authenticated_client fixture provides working JWT auth."""
    resp = await authenticated_client.client.get(
        "/api/v1/auth/me",
        headers=authenticated_client.headers,
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["user_id"] == authenticated_client.user_id


async def test_authenticated_client_user_id_is_valid_uuid(authenticated_client):
    """authenticated_client.user_id is a valid UUID string."""
    UUID(authenticated_client.user_id)


async def test_device_headers_provides_valid_device_auth(device_headers):
    """device_headers fixture provides working device auth for /ping."""
    resp = await device_headers.client.post(
        "/api/v1/ping",
        json={"timezone": "America/New_York", "app_version": "1.0.0"},
        headers=device_headers.headers,
    )
    assert resp.status_code == 200


async def test_device_headers_has_all_expected_fields(device_headers):
    """device_headers fixture exposes all expected attributes."""
    assert "X-Device-Token" in device_headers.headers
    assert "X-Device-Id" in device_headers.headers
    assert device_headers.device_token is not None
    assert device_headers.device_id == "fixture-device-001"
    assert device_headers.user_id is not None
    assert device_headers.auth_headers is not None
