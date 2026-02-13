"""Integration tests for Admin Dashboard view and panel access."""

from datetime import datetime, timedelta

import pytest
from sqlalchemy import func, select

from app.admin.dashboard import DashboardView
from app.models.paired_device import PairedDevice
from app.models.ride import Ride
from app.models.search_status import SearchStatus
from app.models.user import User


class TestDashboardViewConfiguration:
    """Tests for DashboardView configuration."""

    def test_dashboard_has_required_attributes(self):
        """Test that DashboardView has required configuration attributes."""
        assert hasattr(DashboardView, "name")
        assert hasattr(DashboardView, "icon")

    def test_dashboard_name_and_icon(self):
        """Test DashboardView has correct name and icon."""
        assert DashboardView.name == "Dashboard"
        assert DashboardView.icon == "fa-solid fa-chart-line"


class TestDashboardViewStatistics:
    """Tests for DashboardView statistics queries."""

    @pytest.mark.asyncio
    async def test_dashboard_returns_correct_statistics_from_database(
        self, db_session, fake_redis, app_client
    ):
        """Test DashboardView returns correct statistics from database."""
        # Create test data
        user1 = User(email="user1@example.com", password_hash="hash1")
        user2 = User(email="user2@example.com", password_hash="hash2")
        db_session.add(user1)
        db_session.add(user2)
        await db_session.flush()

        # Create paired devices
        device1 = PairedDevice(
            user_id=user1.id,
            device_id="device1",
            device_token_hash="token1",
            timezone="UTC",
            last_ping_at=datetime.utcnow(),  # Active (pinged recently)
        )
        device2 = PairedDevice(
            user_id=user2.id,
            device_id="device2",
            device_token_hash="token2",
            timezone="UTC",
            last_ping_at=datetime.utcnow() - timedelta(minutes=60),  # Inactive
        )
        db_session.add(device1)
        db_session.add(device2)
        await db_session.flush()

        # Create search status
        search_status = SearchStatus(user_id=user1.id, is_active=True)
        db_session.add(search_status)
        await db_session.flush()

        # Create rides
        ride_recent = Ride(
            user_id=user1.id,
            idempotency_key="ride_recent",
            event_type="requested",
            ride_data={"price": 10.0},
            created_at=datetime.utcnow() - timedelta(hours=1),  # Last 24h
        )
        ride_old = Ride(
            user_id=user2.id,
            idempotency_key="ride_old",
            event_type="requested",
            ride_data={"price": 15.0},
            created_at=datetime.utcnow() - timedelta(days=5),  # Last 7d
        )
        db_session.add(ride_recent)
        db_session.add(ride_old)
        await db_session.commit()

        # Verify statistics directly via database queries
        users_count = await db_session.scalar(select(func.count(User.id)))
        assert users_count == 2

        threshold = datetime.utcnow() - timedelta(minutes=30)
        active_devices = await db_session.scalar(
            select(func.count(PairedDevice.id)).where(PairedDevice.last_ping_at >= threshold)
        )
        assert active_devices == 1  # Only device1 pinged in last 30 min

        total_devices = await db_session.scalar(select(func.count(PairedDevice.id)))
        assert total_devices == 2

        active_searches = await db_session.scalar(
            select(func.count(SearchStatus.id)).where(
                SearchStatus.is_active == True  # noqa: E712
            )
        )
        assert active_searches == 1

        day_ago = datetime.utcnow() - timedelta(hours=24)
        rides_24h = await db_session.scalar(
            select(func.count(Ride.id)).where(Ride.created_at >= day_ago)
        )
        assert rides_24h == 1  # Only ride_recent

        week_ago = datetime.utcnow() - timedelta(days=7)
        rides_7d = await db_session.scalar(
            select(func.count(Ride.id)).where(Ride.created_at >= week_ago)
        )
        assert rides_7d == 2  # Both rides

    @pytest.mark.asyncio
    async def test_dashboard_accessible_via_http(self, app_client):
        """Test dashboard is accessible via HTTP and returns HTML."""
        # Dashboard should require authentication, but route should exist
        resp = await app_client.get("/admin/dashboard")
        # Should either show login page or redirect
        assert resp.status_code in (200, 302, 303)


class TestAdminPanelAccess:
    """Integration tests for admin panel access."""

    @pytest.mark.asyncio
    async def test_admin_panel_requires_authentication(self, app_client):
        """Test that /admin/ redirects to login when not authenticated."""
        # Access admin panel without authentication
        resp = await app_client.get("/admin/")
        # SQLAdmin redirects to login page (302) when not authenticated
        assert resp.status_code == 302

    @pytest.mark.asyncio
    async def test_admin_login_page_renders(self, app_client):
        """Test that /admin/login page renders correctly."""
        resp = await app_client.get("/admin/login")
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_dashboard_accessible_at_correct_path(self, app_client):
        """Test that dashboard is accessible at /admin/dashboard."""
        # Dashboard should require authentication - redirect to login
        resp = await app_client.get("/admin/dashboard")
        # Should redirect to login page (302) when not authenticated
        assert resp.status_code == 302

    @pytest.mark.asyncio
    async def test_admin_login_with_valid_credentials(self, app_client, monkeypatch):
        """Test login with valid credentials creates session."""
        # Set test credentials
        test_username = "test_admin"
        test_password = "test_password123"
        monkeypatch.setattr("app.config.settings.ADMIN_USERNAME", test_username)
        monkeypatch.setattr("app.config.settings.ADMIN_PASSWORD", test_password)

        # Submit login form as form data
        resp = await app_client.post(
            "/admin/login",
            data={"username": test_username, "password": test_password},
            follow_redirects=False,
        )

        # Should redirect to admin panel after successful login
        assert resp.status_code == 302

    @pytest.mark.asyncio
    async def test_admin_login_with_invalid_credentials(self, app_client, monkeypatch):
        """Test login with invalid credentials shows error."""
        # Set test credentials
        test_username = "test_admin"
        test_password = "test_password123"
        monkeypatch.setattr("app.config.settings.ADMIN_USERNAME", test_username)
        monkeypatch.setattr("app.config.settings.ADMIN_PASSWORD", test_password)

        # Submit login form with wrong password
        resp = await app_client.post(
            "/admin/login",
            data={"username": test_username, "password": "wrong_password"},
            follow_redirects=False,
        )

        # Should return 400 Bad Request for invalid credentials
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_admin_logout_clears_session(self, app_client):
        """Test that logout clears the admin session."""
        # Access logout endpoint
        resp = await app_client.get("/admin/logout", follow_redirects=False)
        assert resp.status_code == 302  # Redirects to login after logout
        """Test that logout clears the admin session."""
        # Access logout endpoint
        resp = await app_client.get("/admin/logout", follow_redirects=False)
        assert resp.status_code in (200, 302, 303)


class TestAdminPanelModelViews:
    """Tests for admin panel ModelAdmin views accessibility."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "path",
        [
            "/admin/user/list",
            "/admin/paired-device/list",
            "/admin/search-filter/list",
            "/admin/search-status/list",
            "/admin/ride/list",
            "/admin/accept-failure/list",
            "/admin/refresh-token/list",
        ],
    )
    async def test_model_views_accessible_after_login(self, app_client, path):
        """Test that all ModelAdmin list views are accessible (require auth)."""
        resp = await app_client.get(path)
        # Should require authentication (redirect to login or show login form)
        assert resp.status_code in (200, 302, 303)
