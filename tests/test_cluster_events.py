"""Tests for cluster event hooks (SKE-35).

Verifies that cluster state is updated immediately when device eligibility
changes: search deactivation, balance depletion, device offline, and ride found.
"""

import asyncio
import uuid
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from redis.exceptions import RedisError

from app.models.credit_balance import CreditBalance
from app.models.paired_device import PairedDevice
from app.models.search_status import SearchStatus
from app.models.user import User

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_user(email: str = "cluster@example.com") -> User:
    return User(id=uuid.uuid4(), email=email, password_hash="hashed")


def _make_device(user: User, device_id: str = "dev-001") -> PairedDevice:
    return PairedDevice(
        id=uuid.uuid4(),
        user_id=user.id,
        device_id=device_id,
        device_token_hash="a" * 64,
        timezone="America/New_York",
        last_ping_at=datetime.now(UTC),
    )


# ---------------------------------------------------------------------------
# 1. search_service: search deactivation → remove_device_from_cluster
# ---------------------------------------------------------------------------


class TestSearchDeactivationRemovesFromCluster:
    """set_search_active(active=False) should call remove_device_from_cluster."""

    @pytest.mark.asyncio
    async def test_search_stop_calls_remove(self, db_session, fake_redis):
        """Deactivating search removes device from its cluster."""
        from app.services.search_service import set_search_active

        user = _make_user()
        db_session.add(user)
        await db_session.flush()

        device = _make_device(user)
        db_session.add(device)
        status = SearchStatus(user_id=user.id, is_active=True)
        db_session.add(status)
        await db_session.flush()

        with patch(
            "app.services.search_service.remove_device_from_cluster",
            new_callable=AsyncMock,
        ) as mock_remove:
            await set_search_active(db_session, user.id, active=False, redis=fake_redis)

            mock_remove.assert_awaited_once_with(device.device_id, fake_redis)

    @pytest.mark.asyncio
    async def test_search_start_does_not_call_remove(self, db_session, fake_redis):
        """Activating search should NOT call remove_device_from_cluster."""
        from app.services.search_service import set_search_active

        user = _make_user()
        db_session.add(user)
        await db_session.flush()

        device = _make_device(user)
        db_session.add(device)
        status = SearchStatus(user_id=user.id, is_active=False)
        db_session.add(status)
        await db_session.flush()

        with patch(
            "app.services.search_service.remove_device_from_cluster",
            new_callable=AsyncMock,
        ) as mock_remove:
            await set_search_active(db_session, user.id, active=True, redis=fake_redis)

            mock_remove.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_search_stop_without_redis_skips_cluster(self, db_session):
        """When redis is not passed, cluster update is skipped."""
        from app.services.search_service import set_search_active

        user = _make_user()
        db_session.add(user)
        await db_session.flush()

        status = SearchStatus(user_id=user.id, is_active=True)
        db_session.add(status)
        await db_session.flush()

        with patch(
            "app.services.search_service.remove_device_from_cluster",
            new_callable=AsyncMock,
        ) as mock_remove:
            await set_search_active(db_session, user.id, active=False)

            mock_remove.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_search_stop_no_device_skips_cluster(self, db_session, fake_redis):
        """When user has no paired device, cluster removal is skipped."""
        from app.services.search_service import set_search_active

        user = _make_user()
        db_session.add(user)
        await db_session.flush()

        status = SearchStatus(user_id=user.id, is_active=True)
        db_session.add(status)
        await db_session.flush()

        with patch(
            "app.services.search_service.remove_device_from_cluster",
            new_callable=AsyncMock,
        ) as mock_remove:
            await set_search_active(db_session, user.id, active=False, redis=fake_redis)

            mock_remove.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_search_stop_cluster_error_does_not_block(self, db_session, fake_redis):
        """Redis error during cluster removal does not prevent search deactivation."""
        from app.services.search_service import set_search_active

        user = _make_user()
        db_session.add(user)
        await db_session.flush()

        device = _make_device(user)
        db_session.add(device)
        status = SearchStatus(user_id=user.id, is_active=True)
        db_session.add(status)
        await db_session.flush()

        with patch(
            "app.services.search_service.remove_device_from_cluster",
            new_callable=AsyncMock,
            side_effect=RedisError("Redis down"),
        ):
            # Should not raise
            await set_search_active(db_session, user.id, active=False, redis=fake_redis)

        # Verify search was still deactivated
        from sqlalchemy import select

        result = await db_session.execute(
            select(SearchStatus.is_active).where(SearchStatus.user_id == user.id)
        )
        assert result.scalar_one() is False


# ---------------------------------------------------------------------------
# 2. credit_service: balance ≤ 0 → remove_device_from_cluster
# ---------------------------------------------------------------------------


class TestBalanceDepletionRemovesFromCluster:
    """charge_credits should call remove_device_from_cluster when balance reaches 0."""

    @pytest.mark.asyncio
    async def test_balance_zero_calls_remove(self, db_session, fake_redis):
        """Charging to zero balance removes device from cluster."""
        from app.services.credit_service import charge_credits

        user = _make_user()
        db_session.add(user)
        await db_session.flush()

        device = _make_device(user)
        db_session.add(device)
        balance = CreditBalance(user_id=user.id, balance=5)
        db_session.add(balance)
        await db_session.flush()

        with patch(
            "app.services.credit_service.remove_device_from_cluster",
            new_callable=AsyncMock,
        ) as mock_remove:
            _charged, new_balance = await charge_credits(
                user.id, 5, uuid.uuid4(), db_session, fake_redis
            )

            assert new_balance == 0
            mock_remove.assert_awaited_once_with(device.device_id, fake_redis)

    @pytest.mark.asyncio
    async def test_balance_positive_does_not_call_remove(self, db_session, fake_redis):
        """Charging that leaves positive balance should NOT remove from cluster."""
        from app.services.credit_service import charge_credits

        user = _make_user()
        db_session.add(user)
        await db_session.flush()

        device = _make_device(user)
        db_session.add(device)
        balance = CreditBalance(user_id=user.id, balance=10)
        db_session.add(balance)
        await db_session.flush()

        with patch(
            "app.services.credit_service.remove_device_from_cluster",
            new_callable=AsyncMock,
        ) as mock_remove:
            _charged, new_balance = await charge_credits(
                user.id, 5, uuid.uuid4(), db_session, fake_redis
            )

            assert new_balance == 5
            mock_remove.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_balance_zero_no_device_skips_cluster(self, db_session, fake_redis):
        """When user has no paired device, cluster removal is skipped even at zero balance."""
        from app.services.credit_service import charge_credits

        user = _make_user()
        db_session.add(user)
        await db_session.flush()

        balance = CreditBalance(user_id=user.id, balance=5)
        db_session.add(balance)
        await db_session.flush()

        with patch(
            "app.services.credit_service.remove_device_from_cluster",
            new_callable=AsyncMock,
        ) as mock_remove:
            _charged, new_balance = await charge_credits(
                user.id, 5, uuid.uuid4(), db_session, fake_redis
            )

            assert new_balance == 0
            mock_remove.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_balance_zero_cluster_error_does_not_block(self, db_session, fake_redis):
        """Redis error during cluster removal does not block credit charge."""
        from app.services.credit_service import charge_credits

        user = _make_user()
        db_session.add(user)
        await db_session.flush()

        device = _make_device(user)
        db_session.add(device)
        balance = CreditBalance(user_id=user.id, balance=5)
        db_session.add(balance)
        await db_session.flush()

        with patch(
            "app.services.credit_service.remove_device_from_cluster",
            new_callable=AsyncMock,
            side_effect=Exception("Redis down"),
        ):
            charged, new_balance = await charge_credits(
                user.id, 5, uuid.uuid4(), db_session, fake_redis
            )

            # Charge still succeeds
            assert charged == 5
            assert new_balance == 0


# ---------------------------------------------------------------------------
# 3. health_check: device offline → remove_device_from_cluster
# ---------------------------------------------------------------------------


class TestDeviceOfflineRemovesFromCluster:
    """check_device_health should call remove_device_from_cluster when device goes offline."""

    @pytest.mark.asyncio
    async def test_offline_with_fcm_calls_remove(self):
        """Device going offline (FCM sent) removes from cluster."""
        user = _make_user()
        user.fcm_token = "fcm-token-123"
        device = _make_device(user)
        device.last_ping_at = datetime.now(UTC) - timedelta(minutes=60)
        device.offline_notified = False
        device.user = user

        mock_db = AsyncMock()

        @asynccontextmanager
        async def mock_session_factory():
            yield mock_db

        with (
            patch(
                "app.tasks.health_check.AsyncSessionLocal",
                side_effect=mock_session_factory,
            ),
            patch(
                "app.tasks.health_check.get_active_paired_devices",
                new_callable=AsyncMock,
                return_value=[device],
            ),
            patch(
                "app.tasks.health_check.get_user_filters",
                new_callable=AsyncMock,
                return_value=MagicMock(),
            ),
            patch(
                "app.tasks.health_check.should_notify_device_offline",
                return_value=True,
            ),
            patch(
                "app.tasks.health_check.should_reset_offline_notified",
                return_value=False,
            ),
            patch(
                "app.tasks.health_check.send_push",
                new_callable=AsyncMock,
                return_value=True,
            ),
            patch(
                "app.tasks.health_check.remove_device_from_cluster",
                new_callable=AsyncMock,
            ) as mock_remove,
            patch(
                "app.tasks.health_check.asyncio.sleep",
                new_callable=AsyncMock,
                side_effect=asyncio.CancelledError(),
            ),
            patch("app.tasks.health_check.settings") as mock_settings,
        ):
            mock_settings.HEALTH_CHECK_INTERVAL_MINUTES = 5
            mock_settings.OFFLINE_NOTIFICATION_THRESHOLD_MINUTES = 30
            mock_settings.DEFAULT_SEARCH_INTERVAL_SECONDS = 15

            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(
                    asyncio.create_task(
                        __import__(
                            "app.tasks.health_check", fromlist=["check_device_health"]
                        ).check_device_health()
                    ),
                    timeout=5,
                )

            mock_remove.assert_awaited_once_with(device.device_id, mock_remove.call_args[0][1])

    @pytest.mark.asyncio
    async def test_offline_no_fcm_token_calls_remove(self):
        """Device going offline without FCM token still removes from cluster."""
        user = _make_user()
        user.fcm_token = None
        device = _make_device(user)
        device.last_ping_at = datetime.now(UTC) - timedelta(minutes=60)
        device.offline_notified = False
        device.user = user

        mock_db = AsyncMock()

        @asynccontextmanager
        async def mock_session_factory():
            yield mock_db

        with (
            patch(
                "app.tasks.health_check.AsyncSessionLocal",
                side_effect=mock_session_factory,
            ),
            patch(
                "app.tasks.health_check.get_active_paired_devices",
                new_callable=AsyncMock,
                return_value=[device],
            ),
            patch(
                "app.tasks.health_check.get_user_filters",
                new_callable=AsyncMock,
                return_value=MagicMock(),
            ),
            patch(
                "app.tasks.health_check.should_notify_device_offline",
                return_value=True,
            ),
            patch(
                "app.tasks.health_check.should_reset_offline_notified",
                return_value=False,
            ),
            patch(
                "app.tasks.health_check.remove_device_from_cluster",
                new_callable=AsyncMock,
            ) as mock_remove,
            patch(
                "app.tasks.health_check.asyncio.sleep",
                new_callable=AsyncMock,
                side_effect=asyncio.CancelledError(),
            ),
            patch("app.tasks.health_check.settings") as mock_settings,
        ):
            mock_settings.HEALTH_CHECK_INTERVAL_MINUTES = 5
            mock_settings.OFFLINE_NOTIFICATION_THRESHOLD_MINUTES = 30
            mock_settings.DEFAULT_SEARCH_INTERVAL_SECONDS = 15

            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(
                    asyncio.create_task(
                        __import__(
                            "app.tasks.health_check", fromlist=["check_device_health"]
                        ).check_device_health()
                    ),
                    timeout=5,
                )

            mock_remove.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_offline_cluster_error_does_not_block_health_check(self):
        """Redis error during cluster removal does not break the health check loop."""
        user = _make_user()
        user.fcm_token = "fcm-token-123"
        device = _make_device(user)
        device.last_ping_at = datetime.now(UTC) - timedelta(minutes=60)
        device.offline_notified = False
        device.user = user

        mock_db = AsyncMock()

        @asynccontextmanager
        async def mock_session_factory():
            yield mock_db

        with (
            patch(
                "app.tasks.health_check.AsyncSessionLocal",
                side_effect=mock_session_factory,
            ),
            patch(
                "app.tasks.health_check.get_active_paired_devices",
                new_callable=AsyncMock,
                return_value=[device],
            ),
            patch(
                "app.tasks.health_check.get_user_filters",
                new_callable=AsyncMock,
                return_value=MagicMock(),
            ),
            patch(
                "app.tasks.health_check.should_notify_device_offline",
                return_value=True,
            ),
            patch(
                "app.tasks.health_check.should_reset_offline_notified",
                return_value=False,
            ),
            patch(
                "app.tasks.health_check.send_push",
                new_callable=AsyncMock,
                return_value=True,
            ),
            patch(
                "app.tasks.health_check.remove_device_from_cluster",
                new_callable=AsyncMock,
                side_effect=Exception("Redis down"),
            ),
            patch(
                "app.tasks.health_check.asyncio.sleep",
                new_callable=AsyncMock,
                side_effect=asyncio.CancelledError(),
            ),
            patch("app.tasks.health_check.settings") as mock_settings,
        ):
            mock_settings.HEALTH_CHECK_INTERVAL_MINUTES = 5
            mock_settings.OFFLINE_NOTIFICATION_THRESHOLD_MINUTES = 30
            mock_settings.DEFAULT_SEARCH_INTERVAL_SECONDS = 15

            # Should not raise anything other than CancelledError from sleep
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(
                    asyncio.create_task(
                        __import__(
                            "app.tasks.health_check", fromlist=["check_device_health"]
                        ).check_device_health()
                    ),
                    timeout=5,
                )

            # Health check continued despite the error — offline_notified was set
            assert device.offline_notified is True


# ---------------------------------------------------------------------------
# 4. ride_service: ride found → penalize_device_in_cluster
# ---------------------------------------------------------------------------


class TestRideFoundPenalizesDevice:
    """create_ride_with_charge should call penalize_device_in_cluster."""

    @pytest.mark.asyncio
    async def test_ride_creation_calls_penalize(self, db_session, fake_redis):
        """Creating a ride penalizes the device in its cluster."""
        from app.services.ride_service import create_ride_with_charge

        user = _make_user()
        db_session.add(user)
        await db_session.flush()

        device = _make_device(user)
        db_session.add(device)
        balance = CreditBalance(user_id=user.id, balance=100)
        db_session.add(balance)
        await db_session.flush()

        # Mock config service to return a credit cost
        with (
            patch(
                "app.services.ride_service.billing.penalize_device_in_cluster",
                new_callable=AsyncMock,
            ) as mock_penalize,
            patch(
                "app.services.ride_service.billing.get_ride_credit_cost",
                new_callable=AsyncMock,
                return_value=1,
            ),
            patch(
                "app.services.credit_service.remove_device_from_cluster",
                new_callable=AsyncMock,
            ),
        ):
            _ride, _charged, _new_balance = await create_ride_with_charge(
                db_session,
                fake_redis,
                user_id=user.id,
                idempotency_key=str(uuid.uuid4()),
                event_type="ACCEPTED",
                ride_data={"price": 10.0, "pickup_time": "Today · 6:00AM"},
                ride_hash="a" * 64,
                price=10.0,
                verification_deadline=None,
                device_id=device.device_id,
            )

            mock_penalize.assert_awaited_once_with(device.device_id, fake_redis)

    @pytest.mark.asyncio
    async def test_ride_creation_without_device_id_skips_penalize(self, db_session, fake_redis):
        """When device_id is not provided, penalization is skipped."""
        from app.services.ride_service import create_ride_with_charge

        user = _make_user()
        db_session.add(user)
        await db_session.flush()

        balance = CreditBalance(user_id=user.id, balance=100)
        db_session.add(balance)
        await db_session.flush()

        with (
            patch(
                "app.services.ride_service.billing.penalize_device_in_cluster",
                new_callable=AsyncMock,
            ) as mock_penalize,
            patch(
                "app.services.ride_service.billing.get_ride_credit_cost",
                new_callable=AsyncMock,
                return_value=1,
            ),
            patch(
                "app.services.credit_service.remove_device_from_cluster",
                new_callable=AsyncMock,
            ),
        ):
            _ride, _charged, _new_balance = await create_ride_with_charge(
                db_session,
                fake_redis,
                user_id=user.id,
                idempotency_key=str(uuid.uuid4()),
                event_type="ACCEPTED",
                ride_data={"price": 10.0, "pickup_time": "Today · 6:00AM"},
                ride_hash="a" * 64,
                price=10.0,
                verification_deadline=None,
            )

            mock_penalize.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_ride_penalize_error_does_not_block(self, db_session, fake_redis):
        """Redis error during penalization does not block ride creation."""
        from app.services.ride_service import create_ride_with_charge

        user = _make_user()
        db_session.add(user)
        await db_session.flush()

        device = _make_device(user)
        db_session.add(device)
        balance = CreditBalance(user_id=user.id, balance=100)
        db_session.add(balance)
        await db_session.flush()

        with (
            patch(
                "app.services.ride_service.billing.penalize_device_in_cluster",
                new_callable=AsyncMock,
                side_effect=Exception("Redis down"),
            ),
            patch(
                "app.services.ride_service.billing.get_ride_credit_cost",
                new_callable=AsyncMock,
                return_value=1,
            ),
            patch(
                "app.services.credit_service.remove_device_from_cluster",
                new_callable=AsyncMock,
            ),
        ):
            ride, charged, _new_balance = await create_ride_with_charge(
                db_session,
                fake_redis,
                user_id=user.id,
                idempotency_key=str(uuid.uuid4()),
                event_type="ACCEPTED",
                ride_data={"price": 10.0, "pickup_time": "Today · 6:00AM"},
                ride_hash="a" * 64,
                price=10.0,
                verification_deadline=None,
                device_id=device.device_id,
            )

            # Ride was still created successfully
            assert ride is not None
            assert charged == 1
