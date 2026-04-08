"""Tests for the cluster admin map view and API endpoint."""

import json
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, patch

import pytest

from app.admin.cluster_map import ClusterMapView
from app.models.app_config import AppConfig
from app.models.credit_balance import CreditBalance
from app.models.paired_device import PairedDevice
from app.models.ride import Ride
from app.models.user import User


def _make_fake_redis(store: dict[str, str] | None = None):
    """Build a fake Redis mock with scan/smembers support."""
    if store is None:
        store = {}

    sets_store: dict[str, set[str]] = {}

    async def mock_get(key):
        return store.get(key)

    async def mock_scan(cursor=0, match=None, count=200):
        import fnmatch

        matched = [k for k in store if fnmatch.fnmatch(k, match)] if match else list(store)
        return (0, matched)

    async def mock_smembers(key):
        return sets_store.get(key, set())

    redis = AsyncMock()
    redis.get = AsyncMock(side_effect=mock_get)
    redis.scan = AsyncMock(side_effect=mock_scan)
    redis.smembers = AsyncMock(side_effect=mock_smembers)
    redis._store = store
    redis._sets_store = sets_store
    return redis


class TestClusterMapViewConfiguration:
    """Tests for ClusterMapView configuration."""

    def test_has_required_attributes(self):
        assert ClusterMapView.name == "Cluster Map"
        assert ClusterMapView.icon == "fa-solid fa-map-location-dot"


class TestClusterApiEndpoint:
    """Tests for GET /admin/api/clusters."""

    @pytest.mark.asyncio
    async def test_clusters_present(self, db_session, admin_client):
        """API returns correct cluster data when clusters exist in Redis."""
        # Create test users and devices
        user1 = User(email="alice@example.com", password_hash="hash1")
        user2 = User(email="bob@example.com", password_hash="hash2")
        db_session.add_all([user1, user2])
        await db_session.flush()

        device1 = PairedDevice(
            user_id=user1.id,
            device_id="dev1",
            device_token_hash="t1",
            timezone="UTC",
            latitude=40.71,
            longitude=-74.01,
            last_ping_at=datetime.utcnow(),
        )
        device2 = PairedDevice(
            user_id=user2.id,
            device_id="dev2",
            device_token_hash="t2",
            timezone="UTC",
            latitude=40.72,
            longitude=-74.02,
            last_ping_at=datetime.utcnow(),
        )
        db_session.add_all([device1, device2])

        cb1 = CreditBalance(user_id=user1.id, balance=50)
        cb2 = CreditBalance(user_id=user2.id, balance=30)
        db_session.add_all([cb1, cb2])

        # Add config
        db_session.add(AppConfig(key="clustering_enabled", value="true"))
        db_session.add(AppConfig(key="clustering_threshold_miles", value="16"))

        # Add a ride for user1
        ride = Ride(
            user_id=user1.id,
            idempotency_key="ride1",
            event_type="ACCEPTED",
            ride_data={"price": 10},
            ride_hash="a" * 64,
            created_at=datetime.utcnow() - timedelta(hours=1),
        )
        db_session.add(ride)
        await db_session.commit()

        # Set up fake Redis with cluster data
        cluster_id = "test_cluster_1"
        fake_redis = _make_fake_redis(
            {
                f"cluster:{cluster_id}": json.dumps({"active_members": 2, "search_interval": 30}),
                "device_cluster:dev1": json.dumps({"cluster_id": cluster_id, "status": "active"}),
                "device_cluster:dev2": json.dumps(
                    {"cluster_id": cluster_id, "status": "penalized"}
                ),
            }
        )
        fake_redis._sets_store[f"cluster_members:{cluster_id}"] = {"dev1", "dev2"}

        with patch("app.admin.cluster_map.redis_client", fake_redis):
            resp = await admin_client.client.get("/admin/api/clusters")

        assert resp.status_code == 200
        data = resp.json()

        assert data["clustering_enabled"] is True
        assert data["clustering_threshold_miles"] == 16
        assert len(data["clusters"]) == 1
        assert len(data["solo_devices"]) == 0

        cluster = data["clusters"][0]
        assert cluster["cluster_id"] == cluster_id
        assert cluster["active_members"] == 2
        assert cluster["total_members"] == 2
        assert cluster["search_interval"] == 30
        assert "lat" in cluster["centroid"]
        assert "lon" in cluster["centroid"]

        # Check devices in cluster
        device_ids = {d["device_id"] for d in cluster["devices"]}
        assert device_ids == {"dev1", "dev2"}

        dev1_data = next(d for d in cluster["devices"] if d["device_id"] == "dev1")
        assert dev1_data["user_name"] == "alice@example.com"
        assert dev1_data["balance"] == 50
        assert dev1_data["status"] == "active"
        assert dev1_data["last_ride_at"] is not None

        dev2_data = next(d for d in cluster["devices"] if d["device_id"] == "dev2")
        assert dev2_data["status"] == "penalized"

    @pytest.mark.asyncio
    async def test_empty_clusters(self, db_session, admin_client):
        """API returns empty clusters when no cluster data in Redis."""
        db_session.add(AppConfig(key="clustering_enabled", value="true"))
        await db_session.commit()

        fake_redis = _make_fake_redis({})

        with patch("app.admin.cluster_map.redis_client", fake_redis):
            resp = await admin_client.client.get("/admin/api/clusters")

        assert resp.status_code == 200
        data = resp.json()
        assert data["clustering_enabled"] is True
        assert data["clusters"] == []
        assert data["solo_devices"] == []

    @pytest.mark.asyncio
    async def test_clustering_disabled(self, db_session, admin_client):
        """API returns clustering_enabled=false when feature is disabled."""
        db_session.add(AppConfig(key="clustering_enabled", value="false"))
        await db_session.commit()

        fake_redis = _make_fake_redis({})

        with patch("app.admin.cluster_map.redis_client", fake_redis):
            resp = await admin_client.client.get("/admin/api/clusters")

        assert resp.status_code == 200
        data = resp.json()
        assert data["clustering_enabled"] is False
        assert data["clusters"] == []

    @pytest.mark.asyncio
    async def test_clustering_disabled_default(self, db_session, admin_client):
        """API defaults to clustering_enabled=false when no config exists."""
        fake_redis = _make_fake_redis({})

        with patch("app.admin.cluster_map.redis_client", fake_redis):
            resp = await admin_client.client.get("/admin/api/clusters")

        assert resp.status_code == 200
        data = resp.json()
        assert data["clustering_enabled"] is False
        assert data["clustering_threshold_miles"] == 16

    @pytest.mark.asyncio
    async def test_solo_devices_identified(self, db_session, admin_client):
        """Devices with location but not in any cluster appear as solo."""
        user = User(email="solo@example.com", password_hash="hash1")
        db_session.add(user)
        await db_session.flush()

        device = PairedDevice(
            user_id=user.id,
            device_id="solo_dev",
            device_token_hash="t1",
            timezone="UTC",
            latitude=41.88,
            longitude=-87.63,
            last_ping_at=datetime.utcnow(),
        )
        db_session.add(device)
        db_session.add(CreditBalance(user_id=user.id, balance=25))
        db_session.add(AppConfig(key="clustering_enabled", value="true"))
        await db_session.commit()

        fake_redis = _make_fake_redis({})

        with patch("app.admin.cluster_map.redis_client", fake_redis):
            resp = await admin_client.client.get("/admin/api/clusters")

        assert resp.status_code == 200
        data = resp.json()
        assert len(data["clusters"]) == 0
        assert len(data["solo_devices"]) == 1

        solo = data["solo_devices"][0]
        assert solo["device_id"] == "solo_dev"
        assert solo["user_name"] == "solo@example.com"
        assert solo["latitude"] == 41.88
        assert solo["longitude"] == -87.63
        assert solo["balance"] == 25
        assert solo["last_ping_at"] is not None

    @pytest.mark.asyncio
    async def test_devices_without_location_excluded(self, db_session, admin_client):
        """Devices without latitude are excluded from both clusters and solo."""
        user = User(email="noloc@example.com", password_hash="hash1")
        db_session.add(user)
        await db_session.flush()

        device = PairedDevice(
            user_id=user.id,
            device_id="noloc_dev",
            device_token_hash="t1",
            timezone="UTC",
            latitude=None,
            longitude=None,
        )
        db_session.add(device)
        db_session.add(AppConfig(key="clustering_enabled", value="true"))
        await db_session.commit()

        fake_redis = _make_fake_redis({})

        with patch("app.admin.cluster_map.redis_client", fake_redis):
            resp = await admin_client.client.get("/admin/api/clusters")

        assert resp.status_code == 200
        data = resp.json()
        assert data["clusters"] == []
        assert data["solo_devices"] == []

    @pytest.mark.asyncio
    async def test_response_format_validation(self, db_session, admin_client):
        """API response contains all required top-level fields."""
        fake_redis = _make_fake_redis({})

        with patch("app.admin.cluster_map.redis_client", fake_redis):
            resp = await admin_client.client.get("/admin/api/clusters")

        assert resp.status_code == 200
        data = resp.json()
        assert "clustering_enabled" in data
        assert "clustering_threshold_miles" in data
        assert "clusters" in data
        assert "solo_devices" in data
        assert isinstance(data["clusters"], list)
        assert isinstance(data["solo_devices"], list)


class TestClusterMapAuth:
    """Tests for cluster map authentication requirements."""

    @pytest.mark.asyncio
    async def test_cluster_map_requires_auth(self, app_client):
        """Cluster map page redirects to login when not authenticated."""
        resp = await app_client.get("/admin/cluster-map")
        assert resp.status_code == 302

    @pytest.mark.asyncio
    async def test_api_clusters_requires_auth(self, app_client):
        """API endpoint redirects to login when not authenticated."""
        resp = await app_client.get("/admin/api/clusters")
        assert resp.status_code == 302

    @pytest.mark.asyncio
    async def test_cluster_map_accessible_after_login(self, admin_client):
        """Cluster map page is accessible after admin login."""
        resp = await admin_client.client.get("/admin/cluster-map")
        assert resp.status_code == 200
        assert "Cluster Map" in resp.text
