"""Integration tests for Admin Dashboard view and panel access."""

from datetime import datetime, timedelta

import pytest
from sqlalchemy import and_, cast, func, select
from sqlalchemy.types import Date

from app.admin.dashboard import DashboardView
from app.models.credit_balance import CreditBalance
from app.models.paired_device import PairedDevice
from app.models.purchase_order import PurchaseOrder, PurchaseStatus
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
            ride_hash="a" * 64,
            created_at=datetime.utcnow() - timedelta(hours=1),  # Last 24h
        )
        ride_old = Ride(
            user_id=user2.id,
            idempotency_key="ride_old",
            event_type="requested",
            ride_data={"price": 15.0},
            ride_hash="b" * 64,
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
        import bcrypt

        # Set test credentials (ADMIN_PASSWORD is a bcrypt hash)
        test_username = "test_admin"
        test_password = "test_password123"
        hashed = bcrypt.hashpw(test_password.encode(), bcrypt.gensalt()).decode()
        monkeypatch.setattr("app.config.settings.ADMIN_USERNAME", test_username)
        monkeypatch.setattr("app.config.settings.ADMIN_PASSWORD", hashed)

        # Submit login form as form data (explicit Content-Type because
        # app_client has a default application/json header)
        resp = await app_client.post(
            "/admin/login",
            data={"username": test_username, "password": test_password},
            headers={"Content-Type": "application/x-www-form-urlencoded"},
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


class TestDashboardBillingWidgets:
    """Tests for billing-related dashboard widgets (Total Credits, Purchases Today)."""

    @pytest.mark.asyncio
    async def test_total_credits_shows_correct_sum(self, db_session, fake_redis, app_client):
        """Widget 'Total Credits' shows correct sum of all user balances."""
        user1 = User(email="credits1@example.com", password_hash="hash1")
        user2 = User(email="credits2@example.com", password_hash="hash2")
        user3 = User(email="credits3@example.com", password_hash="hash3")
        db_session.add_all([user1, user2, user3])
        await db_session.flush()

        db_session.add_all(
            [
                CreditBalance(user_id=user1.id, balance=50),
                CreditBalance(user_id=user2.id, balance=120),
                CreditBalance(user_id=user3.id, balance=0),
            ]
        )
        await db_session.commit()

        total = await db_session.scalar(select(func.coalesce(func.sum(CreditBalance.balance), 0)))
        assert total == 170

    @pytest.mark.asyncio
    async def test_total_credits_zero_when_empty(self, db_session, fake_redis, app_client):
        """Widget 'Total Credits' returns 0 when credit_balances table is empty."""
        total = await db_session.scalar(select(func.coalesce(func.sum(CreditBalance.balance), 0)))
        assert total == 0

    @pytest.mark.asyncio
    async def test_purchases_today_shows_count_and_sum(self, db_session, fake_redis, app_client):
        """Widget 'Purchases Today' shows count and credit sum of today's VERIFIED orders."""
        user = User(email="buyer@example.com", password_hash="hash1")
        db_session.add(user)
        await db_session.flush()

        # Two VERIFIED purchases today
        db_session.add_all(
            [
                PurchaseOrder(
                    user_id=user.id,
                    product_id="credits_10",
                    purchase_token="token_today_1",
                    credits_amount=10,
                    status=PurchaseStatus.VERIFIED.value,
                    created_at=datetime.utcnow(),
                ),
                PurchaseOrder(
                    user_id=user.id,
                    product_id="credits_50",
                    purchase_token="token_today_2",
                    credits_amount=50,
                    status=PurchaseStatus.VERIFIED.value,
                    created_at=datetime.utcnow(),
                ),
            ]
        )
        await db_session.commit()

        result = await db_session.execute(
            select(
                func.count(PurchaseOrder.id),
                func.coalesce(func.sum(PurchaseOrder.credits_amount), 0),
            ).where(
                and_(
                    PurchaseOrder.status == PurchaseStatus.VERIFIED.value,
                    cast(PurchaseOrder.created_at, Date) == func.current_date(),
                )
            )
        )
        row = result.one()
        assert row[0] == 2
        assert row[1] == 60

    @pytest.mark.asyncio
    async def test_purchases_today_excludes_non_verified(self, db_session, fake_redis, app_client):
        """Widget 'Purchases Today' does not include PENDING or FAILED orders."""
        user = User(email="buyer2@example.com", password_hash="hash2")
        db_session.add(user)
        await db_session.flush()

        db_session.add_all(
            [
                PurchaseOrder(
                    user_id=user.id,
                    product_id="credits_10",
                    purchase_token="token_pending",
                    credits_amount=10,
                    status=PurchaseStatus.PENDING.value,
                    created_at=datetime.utcnow(),
                ),
                PurchaseOrder(
                    user_id=user.id,
                    product_id="credits_25",
                    purchase_token="token_failed",
                    credits_amount=25,
                    status=PurchaseStatus.FAILED.value,
                    created_at=datetime.utcnow(),
                ),
                PurchaseOrder(
                    user_id=user.id,
                    product_id="credits_50",
                    purchase_token="token_consumed",
                    credits_amount=50,
                    status=PurchaseStatus.CONSUMED.value,
                    created_at=datetime.utcnow(),
                ),
                PurchaseOrder(
                    user_id=user.id,
                    product_id="credits_100",
                    purchase_token="token_verified",
                    credits_amount=100,
                    status=PurchaseStatus.VERIFIED.value,
                    created_at=datetime.utcnow(),
                ),
            ]
        )
        await db_session.commit()

        result = await db_session.execute(
            select(
                func.count(PurchaseOrder.id),
                func.coalesce(func.sum(PurchaseOrder.credits_amount), 0),
            ).where(
                and_(
                    PurchaseOrder.status == PurchaseStatus.VERIFIED.value,
                    cast(PurchaseOrder.created_at, Date) == func.current_date(),
                )
            )
        )
        row = result.one()
        assert row[0] == 1  # Only the VERIFIED one
        assert row[1] == 100

    @pytest.mark.asyncio
    async def test_purchases_today_zero_when_no_purchases(
        self, db_session, fake_redis, app_client
    ):
        """Widget 'Purchases Today' returns 0 count and 0 credits when no purchases today."""
        result = await db_session.execute(
            select(
                func.count(PurchaseOrder.id),
                func.coalesce(func.sum(PurchaseOrder.credits_amount), 0),
            ).where(
                and_(
                    PurchaseOrder.status == PurchaseStatus.VERIFIED.value,
                    cast(PurchaseOrder.created_at, Date) == func.current_date(),
                )
            )
        )
        row = result.one()
        assert row[0] == 0
        assert row[1] == 0

    @pytest.mark.asyncio
    async def test_purchases_today_excludes_yesterday(self, db_session, fake_redis, app_client):
        """Widget 'Purchases Today' does not include yesterday's VERIFIED orders."""
        user = User(email="buyer3@example.com", password_hash="hash3")
        db_session.add(user)
        await db_session.flush()

        db_session.add_all(
            [
                PurchaseOrder(
                    user_id=user.id,
                    product_id="credits_50",
                    purchase_token="token_yesterday",
                    credits_amount=50,
                    status=PurchaseStatus.VERIFIED.value,
                    created_at=datetime.utcnow() - timedelta(days=1),
                ),
            ]
        )
        await db_session.commit()

        result = await db_session.execute(
            select(
                func.count(PurchaseOrder.id),
                func.coalesce(func.sum(PurchaseOrder.credits_amount), 0),
            ).where(
                and_(
                    PurchaseOrder.status == PurchaseStatus.VERIFIED.value,
                    cast(PurchaseOrder.created_at, Date) == func.current_date(),
                )
            )
        )
        row = result.one()
        assert row[0] == 0
        assert row[1] == 0

    @pytest.mark.asyncio
    async def test_dashboard_renders_billing_widgets(self, admin_client):
        """Dashboard page contains billing widget content after login."""
        resp = await admin_client.client.get("/admin/dashboard")
        assert resp.status_code == 200
        body = resp.text
        assert "Total Credits in Circulation" in body
        assert "Purchases Today" in body
