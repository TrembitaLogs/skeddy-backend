"""Tests for app/services/cluster_service.py.

Covers UnionFind, build_clusters, remove_device_from_cluster,
penalize_device_in_cluster, cluster_gate, and haversine_miles.
"""

import json
import math
from unittest.mock import AsyncMock, patch

import pytest
from redis.exceptions import RedisError

from app.services.cluster_service import (
    UnionFind,
    build_clusters,
    cluster_gate,
    haversine_miles,
    penalize_device_in_cluster,
    remove_device_from_cluster,
    write_clusters_to_redis,
)

# ---------------------------------------------------------------------------
# Helper: build a fake Redis with set and pipeline support
# ---------------------------------------------------------------------------


def _make_cluster_redis():
    """Build a fake Redis supporting get/set/setex/delete/smembers/srem/sadd/pipeline/eval."""
    store: dict[str, str] = {}
    sets_store: dict[str, set[str]] = {}

    async def mock_get(key):
        return store.get(key)

    async def mock_setex(key, ttl, value):
        store[key] = value

    async def mock_delete(*keys):
        count = 0
        for key in keys:
            if key in store:
                del store[key]
                count += 1
            if key in sets_store:
                del sets_store[key]
                count += 1
        return count

    async def mock_ttl(key):
        return 300 if key in store else -2

    async def mock_smembers(key):
        return sets_store.get(key, set())

    async def mock_srem(key, *members):
        if key in sets_store:
            removed = 0
            for m in members:
                if m in sets_store[key]:
                    sets_store[key].discard(m)
                    removed += 1
            return removed
        return 0

    async def mock_sadd(key, *members):
        if key not in sets_store:
            sets_store[key] = set()
        added = 0
        for m in members:
            if m not in sets_store[key]:
                sets_store[key].add(m)
                added += 1
        return added

    async def mock_expire(key, ttl):
        return 1 if key in store or key in sets_store else 0

    async def mock_eval(script, numkeys, *args):
        if numkeys == 1:
            # CLUSTER_GATE_LUA emulation
            key = args[0]
            now = float(args[1])
            interval = float(args[2])

            last = store.get(key)
            if last is None or (now - float(last)) >= interval:
                store[key] = str(now)
                return 1
            else:
                return math.ceil(interval - (now - float(last)))

        elif numkeys == 4:
            # REMOVE_DEVICE_LUA emulation
            dc_key, cluster_key, members_key, last_search_key = (
                args[0],
                args[1],
                args[2],
                args[3],
            )
            device_id = args[4]
            _default_ttl = int(args[5])

            store.pop(dc_key, None)
            if members_key in sets_store:
                sets_store[members_key].discard(device_id)

            cluster_raw = store.get(cluster_key)
            if cluster_raw is None:
                return 0

            cluster_data = json.loads(cluster_raw)
            active = max(0, cluster_data.get("active_members", 1) - 1)

            if active <= 0:
                store.pop(cluster_key, None)
                store.pop(last_search_key, None)
                sets_store.pop(members_key, None)
                return 2
            else:
                cluster_data["active_members"] = active
                store[cluster_key] = json.dumps(cluster_data)
                return 1

        elif numkeys == 3:
            # PENALIZE_DEVICE_LUA emulation
            dc_key, cluster_key, members_key = args[0], args[1], args[2]
            _device_id = args[3]
            _default_ttl = int(args[4])
            dc_prefix = args[5]

            dc_raw = store.get(dc_key)
            if dc_raw is None:
                return 0

            dc_data = json.loads(dc_raw)
            dc_data["status"] = "penalized"
            store[dc_key] = json.dumps(dc_data)

            cluster_raw = store.get(cluster_key)
            if cluster_raw is None:
                return 1

            cluster_data = json.loads(cluster_raw)
            active = max(0, cluster_data.get("active_members", 1) - 1)

            if active <= 0:
                member_ids = sets_store.get(members_key, set())
                total = len(member_ids)
                for mid in member_ids:
                    mid_key = dc_prefix + mid
                    mid_raw = store.get(mid_key)
                    if mid_raw is not None:
                        mid_data = json.loads(mid_raw)
                        mid_data["status"] = "active"
                        store[mid_key] = json.dumps(mid_data)
                cluster_data["active_members"] = total
            else:
                cluster_data["active_members"] = active

            store[cluster_key] = json.dumps(cluster_data)
            return 2 if active <= 0 else 1

        return None

    def mock_pipeline():
        """Return a fake pipeline that collects and executes commands."""
        commands = []

        class FakePipeline:
            def setex(self, key, ttl, value):
                commands.append(("setex", key, ttl, value))
                return self

            def delete(self, *keys):
                commands.append(("delete", *keys))
                return self

            def sadd(self, key, *members):
                commands.append(("sadd", key, *members))
                return self

            def expire(self, key, ttl):
                commands.append(("expire", key, ttl))
                return self

            async def execute(self):
                results = []
                for cmd in commands:
                    if cmd[0] == "setex":
                        store[cmd[1]] = cmd[3]
                        results.append(True)
                    elif cmd[0] == "delete":
                        for k in cmd[1:]:
                            store.pop(k, None)
                            sets_store.pop(k, None)
                        results.append(True)
                    elif cmd[0] == "sadd":
                        key = cmd[1]
                        if key not in sets_store:
                            sets_store[key] = set()
                        for m in cmd[2:]:
                            sets_store[key].add(m)
                        results.append(len(cmd) - 2)
                    elif cmd[0] == "expire":
                        results.append(1)
                return results

        return FakePipeline()

    redis = AsyncMock()
    redis.get = AsyncMock(side_effect=mock_get)
    redis.setex = AsyncMock(side_effect=mock_setex)
    redis.delete = AsyncMock(side_effect=mock_delete)
    redis.ttl = AsyncMock(side_effect=mock_ttl)
    redis.smembers = AsyncMock(side_effect=mock_smembers)
    redis.srem = AsyncMock(side_effect=mock_srem)
    redis.sadd = AsyncMock(side_effect=mock_sadd)
    redis.expire = AsyncMock(side_effect=mock_expire)
    redis.eval = AsyncMock(side_effect=mock_eval)
    redis.pipeline = mock_pipeline
    redis._store = store
    redis._sets_store = sets_store
    return redis


# ===========================================================================
# UnionFind tests
# ===========================================================================


class TestUnionFind:
    def test_find_initial(self):
        uf = UnionFind(["a", "b", "c"])
        assert uf.find("a") == "a"
        assert uf.find("b") == "b"

    def test_union_and_find(self):
        uf = UnionFind(["a", "b", "c"])
        uf.union("a", "b")
        assert uf.find("a") == uf.find("b")

    def test_path_compression(self):
        uf = UnionFind(["a", "b", "c", "d"])
        uf.union("a", "b")
        uf.union("b", "c")
        uf.union("c", "d")
        root = uf.find("d")
        # After find with path compression, d should point closer to root
        assert uf.find("d") == root
        assert uf.find("a") == root

    def test_union_same_element(self):
        uf = UnionFind(["x", "y"])
        uf.union("x", "x")
        assert uf.find("x") == "x"

    def test_transitive_union(self):
        uf = UnionFind(["a", "b", "c"])
        uf.union("a", "b")
        uf.union("b", "c")
        assert uf.find("a") == uf.find("c")

    def test_disjoint_sets(self):
        uf = UnionFind(["a", "b", "c", "d"])
        uf.union("a", "b")
        uf.union("c", "d")
        assert uf.find("a") == uf.find("b")
        assert uf.find("c") == uf.find("d")
        assert uf.find("a") != uf.find("c")


# ===========================================================================
# haversine_miles tests
# ===========================================================================


class TestHaversineMiles:
    def test_same_point(self):
        assert haversine_miles(40.0, -74.0, 40.0, -74.0) == pytest.approx(0.0, abs=0.001)

    def test_known_distance(self):
        # NYC to LA is approximately 2451 miles
        dist = haversine_miles(40.7128, -74.0060, 33.9425, -118.4081)
        assert 2400 < dist < 2500

    def test_short_distance(self):
        # Two nearby points (~1 mile apart)
        dist = haversine_miles(40.7128, -74.0060, 40.7260, -74.0060)
        assert 0.5 < dist < 2.0


# ===========================================================================
# build_clusters tests
# ===========================================================================


class TestBuildClusters:
    @pytest.fixture
    def redis(self):
        return _make_cluster_redis()

    async def test_empty_devices(self, redis):
        result = await build_clusters([], threshold_miles=16, redis=redis)
        assert result == {}

    async def test_solo_device_excluded(self, redis):
        devices = [{"device_id": "d1", "lat": 40.7128, "lon": -74.0060}]
        with patch("app.services.cluster_service.h3") as mock_h3:
            mock_h3.latlng_to_cell.return_value = "cell_a"
            mock_h3.grid_ring.return_value = ["cell_x"]  # no occupied neighbor
            result = await build_clusters(devices, threshold_miles=16, redis=redis)
        assert result == {}

    async def test_two_device_cluster(self, redis):
        devices = [
            {"device_id": "d1", "lat": 40.7128, "lon": -74.0060},
            {"device_id": "d2", "lat": 40.7130, "lon": -74.0062},
        ]
        with patch("app.services.cluster_service.h3") as mock_h3:
            # Both in the same cell
            mock_h3.latlng_to_cell.return_value = "cell_a"
            mock_h3.grid_ring.return_value = []
            result = await build_clusters(devices, threshold_miles=16, redis=redis)

        assert len(result) == 1
        cluster_members = next(iter(result.values()))
        assert len(cluster_members) == 2

    async def test_multi_cell_merge(self, redis):
        devices = [
            {"device_id": "d1", "lat": 40.71, "lon": -74.00},
            {"device_id": "d2", "lat": 40.72, "lon": -74.01},
        ]
        with patch("app.services.cluster_service.h3") as mock_h3:
            # Different cells but neighbors within threshold
            mock_h3.latlng_to_cell.side_effect = ["cell_a", "cell_b"]
            mock_h3.grid_ring.side_effect = [
                ["cell_b"],  # cell_a neighbors
                ["cell_a"],  # cell_b neighbors
            ]
            result = await build_clusters(devices, threshold_miles=16, redis=redis)

        assert len(result) == 1
        cluster_members = next(iter(result.values()))
        assert len(cluster_members) == 2

    async def test_threshold_filtering(self, redis):
        # NYC and LA — far apart, should NOT cluster
        devices = [
            {"device_id": "d1", "lat": 40.7128, "lon": -74.0060},
            {"device_id": "d2", "lat": 33.9425, "lon": -118.4081},
        ]
        with patch("app.services.cluster_service.h3") as mock_h3:
            mock_h3.latlng_to_cell.side_effect = ["cell_a", "cell_b"]
            mock_h3.grid_ring.side_effect = [
                ["cell_b"],  # cell_a neighbors (artificially adjacent)
                ["cell_a"],
            ]
            result = await build_clusters(devices, threshold_miles=16, redis=redis)

        # Distance exceeds threshold, so no cluster
        assert result == {}

    async def test_three_devices_two_cells(self, redis):
        devices = [
            {"device_id": "d1", "lat": 40.71, "lon": -74.00},
            {"device_id": "d2", "lat": 40.71, "lon": -74.00},
            {"device_id": "d3", "lat": 40.72, "lon": -74.01},
        ]
        with patch("app.services.cluster_service.h3") as mock_h3:
            mock_h3.latlng_to_cell.side_effect = ["cell_a", "cell_a", "cell_b"]
            mock_h3.grid_ring.side_effect = [
                ["cell_b"],
                ["cell_a"],
            ]
            result = await build_clusters(devices, threshold_miles=16, redis=redis)

        assert len(result) == 1
        cluster_members = next(iter(result.values()))
        assert len(cluster_members) == 3


# ===========================================================================
# write_clusters_to_redis tests
# ===========================================================================


class TestWriteClustersToRedis:
    async def test_writes_all_keys(self):
        redis = _make_cluster_redis()
        clusters = {
            "cluster_1": [
                {"device_id": "d1", "lat": 40.71, "lon": -74.00},
                {"device_id": "d2", "lat": 40.72, "lon": -74.01},
            ]
        }
        device_statuses = {"d1": "active", "d2": "penalized"}
        cluster_params = {"cluster_1": {"active_members": 1, "search_interval": 15}}

        await write_clusters_to_redis(clusters, device_statuses, cluster_params, redis)

        # Verify cluster key
        cluster_data = json.loads(redis._store["cluster:cluster_1"])
        assert cluster_data["active_members"] == 1
        assert cluster_data["search_interval"] == 15

        # Verify device keys
        d1_data = json.loads(redis._store["device_cluster:d1"])
        assert d1_data["cluster_id"] == "cluster_1"
        assert d1_data["status"] == "active"

        d2_data = json.loads(redis._store["device_cluster:d2"])
        assert d2_data["status"] == "penalized"

        # Verify members set
        assert redis._sets_store["cluster_members:cluster_1"] == {"d1", "d2"}


# ===========================================================================
# remove_device_from_cluster tests
# ===========================================================================


class TestRemoveDeviceFromCluster:
    async def test_solo_device_noop(self):
        redis = _make_cluster_redis()
        # No device_cluster key → solo device
        await remove_device_from_cluster("d1", redis)
        # Should not raise, nothing changed

    async def test_normal_removal(self):
        redis = _make_cluster_redis()
        redis._store["device_cluster:d1"] = json.dumps({"cluster_id": "c1", "status": "active"})
        redis._store["cluster:c1"] = json.dumps({"active_members": 2, "search_interval": 15})
        redis._sets_store["cluster_members:c1"] = {"d1", "d2"}

        await remove_device_from_cluster("d1", redis)

        # device key should be removed
        assert "device_cluster:d1" not in redis._store
        # active_members decremented
        cluster_data = json.loads(redis._store["cluster:c1"])
        assert cluster_data["active_members"] == 1

    async def test_last_member_cleanup(self):
        redis = _make_cluster_redis()
        redis._store["device_cluster:d1"] = json.dumps({"cluster_id": "c1", "status": "active"})
        redis._store["cluster:c1"] = json.dumps({"active_members": 1, "search_interval": 15})
        redis._store["cluster_last_search:c1"] = "1234567890"
        redis._sets_store["cluster_members:c1"] = {"d1"}

        await remove_device_from_cluster("d1", redis)

        assert "device_cluster:d1" not in redis._store
        assert "cluster:c1" not in redis._store
        assert "cluster_last_search:c1" not in redis._store

    async def test_redis_error_graceful(self):
        redis = _make_cluster_redis()
        redis.get = AsyncMock(side_effect=RedisError("connection refused"))

        # Should not raise
        await remove_device_from_cluster("d1", redis)

    async def test_invalid_data_graceful(self):
        redis = _make_cluster_redis()
        redis._store["device_cluster:d1"] = "not-json"

        # Should not raise
        await remove_device_from_cluster("d1", redis)


# ===========================================================================
# penalize_device_in_cluster tests
# ===========================================================================


class TestPenalizeDeviceInCluster:
    async def test_solo_device_noop(self):
        redis = _make_cluster_redis()
        await penalize_device_in_cluster("d1", redis)
        # No error, nothing to penalize

    async def test_normal_penalty(self):
        redis = _make_cluster_redis()
        redis._store["device_cluster:d1"] = json.dumps({"cluster_id": "c1", "status": "active"})
        redis._store["device_cluster:d2"] = json.dumps({"cluster_id": "c1", "status": "active"})
        redis._store["cluster:c1"] = json.dumps({"active_members": 2, "search_interval": 15})
        redis._sets_store["cluster_members:c1"] = {"d1", "d2"}

        await penalize_device_in_cluster("d1", redis)

        # d1 status should be penalized
        d1_data = json.loads(redis._store["device_cluster:d1"])
        assert d1_data["status"] == "penalized"

        # active_members decremented
        cluster_data = json.loads(redis._store["cluster:c1"])
        assert cluster_data["active_members"] == 1

        # d2 still active
        d2_data = json.loads(redis._store["device_cluster:d2"])
        assert d2_data["status"] == "active"

    async def test_all_penalized_reset(self):
        redis = _make_cluster_redis()
        redis._store["device_cluster:d1"] = json.dumps({"cluster_id": "c1", "status": "penalized"})
        redis._store["device_cluster:d2"] = json.dumps({"cluster_id": "c1", "status": "active"})
        redis._store["cluster:c1"] = json.dumps({"active_members": 1, "search_interval": 15})
        redis._sets_store["cluster_members:c1"] = {"d1", "d2"}

        # Penalize the last active member
        await penalize_device_in_cluster("d2", redis)

        # All should be reset to active
        d1_data = json.loads(redis._store["device_cluster:d1"])
        assert d1_data["status"] == "active"
        d2_data = json.loads(redis._store["device_cluster:d2"])
        assert d2_data["status"] == "active"

        # active_members should be restored
        cluster_data = json.loads(redis._store["cluster:c1"])
        assert cluster_data["active_members"] == 2

    async def test_redis_error_graceful(self):
        redis = _make_cluster_redis()
        redis.get = AsyncMock(side_effect=RedisError("connection refused"))

        await penalize_device_in_cluster("d1", redis)
        # Should not raise

    async def test_invalid_data_graceful(self):
        redis = _make_cluster_redis()
        redis._store["device_cluster:d1"] = "invalid-json"

        await penalize_device_in_cluster("d1", redis)
        # Should not raise


# ===========================================================================
# cluster_gate tests
# ===========================================================================


class TestClusterGate:
    async def test_clustering_disabled(self):
        redis = _make_cluster_redis()
        result = await cluster_gate("d1", redis, clustering_enabled=False)
        assert result is None

    async def test_solo_device(self):
        redis = _make_cluster_redis()
        # No device_cluster key → solo
        result = await cluster_gate("d1", redis, clustering_enabled=True)
        assert result is None

    async def test_penalized_device(self):
        redis = _make_cluster_redis()
        redis._store["device_cluster:d1"] = json.dumps({"cluster_id": "c1", "status": "penalized"})

        result = await cluster_gate("d1", redis, clustering_enabled=True)
        assert result == {"search": False, "interval_seconds": 60}

    async def test_active_device_search(self):
        redis = _make_cluster_redis()
        redis._store["device_cluster:d1"] = json.dumps({"cluster_id": "c1", "status": "active"})
        redis._store["cluster:c1"] = json.dumps({"active_members": 3, "search_interval": 15})
        # No last search timestamp — first search wins

        result = await cluster_gate("d1", redis, clustering_enabled=True)
        assert result is not None
        assert result["search"] is True
        assert result["interval_seconds"] == 45  # 15 * 3

    async def test_active_device_wait(self):
        redis = _make_cluster_redis()
        redis._store["device_cluster:d1"] = json.dumps({"cluster_id": "c1", "status": "active"})
        redis._store["cluster:c1"] = json.dumps({"active_members": 2, "search_interval": 30})
        # Set a recent last search timestamp
        import time

        redis._store["cluster_last_search:c1"] = str(time.time() - 5)

        result = await cluster_gate("d1", redis, clustering_enabled=True)
        assert result is not None
        assert result["search"] is False
        assert result["interval_seconds"] > 0

    async def test_redis_error_fallback_to_solo(self):
        redis = _make_cluster_redis()
        redis.get = AsyncMock(side_effect=RedisError("connection refused"))

        result = await cluster_gate("d1", redis, clustering_enabled=True)
        assert result is None

    async def test_cluster_expired(self):
        redis = _make_cluster_redis()
        redis._store["device_cluster:d1"] = json.dumps({"cluster_id": "c1", "status": "active"})
        # No cluster:c1 key — expired

        result = await cluster_gate("d1", redis, clustering_enabled=True)
        assert result is None

    async def test_invalid_device_data(self):
        redis = _make_cluster_redis()
        redis._store["device_cluster:d1"] = "not-json"

        result = await cluster_gate("d1", redis, clustering_enabled=True)
        assert result is None

    async def test_redis_error_during_lua(self):
        redis = _make_cluster_redis()
        redis._store["device_cluster:d1"] = json.dumps({"cluster_id": "c1", "status": "active"})
        redis._store["cluster:c1"] = json.dumps({"active_members": 2, "search_interval": 15})
        redis.eval = AsyncMock(side_effect=RedisError("Lua error"))

        result = await cluster_gate("d1", redis, clustering_enabled=True)
        assert result is None


# ===========================================================================
# Concurrent scenario tests — verify Lua atomicity logic
# ===========================================================================


class TestConcurrentRemoveDevice:
    """Verify that concurrent removals produce correct active_members counts."""

    async def test_two_concurrent_removals_from_three_member_cluster(self):
        redis = _make_cluster_redis()
        redis._store["device_cluster:d1"] = json.dumps({"cluster_id": "c1", "status": "active"})
        redis._store["device_cluster:d2"] = json.dumps({"cluster_id": "c1", "status": "active"})
        redis._store["device_cluster:d3"] = json.dumps({"cluster_id": "c1", "status": "active"})
        redis._store["cluster:c1"] = json.dumps({"active_members": 3, "search_interval": 15})
        redis._sets_store["cluster_members:c1"] = {"d1", "d2", "d3"}

        # Sequential removals — each atomically decrements active_members
        await remove_device_from_cluster("d1", redis)
        await remove_device_from_cluster("d2", redis)

        # d1/d2 removed, but d3 still present → active_members=1, cluster still alive
        assert "device_cluster:d1" not in redis._store
        assert "device_cluster:d2" not in redis._store
        cluster_data = json.loads(redis._store["cluster:c1"])
        assert cluster_data["active_members"] == 1

        # Removing the last member triggers full cleanup
        await remove_device_from_cluster("d3", redis)
        assert "cluster:c1" not in redis._store
        assert "device_cluster:d3" not in redis._store

    async def test_removal_preserves_remaining_members(self):
        redis = _make_cluster_redis()
        redis._store["device_cluster:d1"] = json.dumps({"cluster_id": "c1", "status": "active"})
        redis._store["device_cluster:d2"] = json.dumps({"cluster_id": "c1", "status": "active"})
        redis._store["device_cluster:d3"] = json.dumps({"cluster_id": "c1", "status": "active"})
        redis._store["cluster:c1"] = json.dumps({"active_members": 3, "search_interval": 15})
        redis._sets_store["cluster_members:c1"] = {"d1", "d2", "d3"}

        await remove_device_from_cluster("d1", redis)

        cluster_data = json.loads(redis._store["cluster:c1"])
        assert cluster_data["active_members"] == 2
        assert "d1" not in redis._sets_store["cluster_members:c1"]
        assert "d2" in redis._sets_store["cluster_members:c1"]

    async def test_removal_with_expired_cluster_key(self):
        redis = _make_cluster_redis()
        redis._store["device_cluster:d1"] = json.dumps({"cluster_id": "c1", "status": "active"})
        redis._sets_store["cluster_members:c1"] = {"d1", "d2"}
        # cluster:c1 key expired — not in store

        await remove_device_from_cluster("d1", redis)

        # Device key removed, but no cluster to update
        assert "device_cluster:d1" not in redis._store

    async def test_removal_redis_error_on_eval(self):
        redis = _make_cluster_redis()
        redis._store["device_cluster:d1"] = json.dumps({"cluster_id": "c1", "status": "active"})
        redis._store["cluster:c1"] = json.dumps({"active_members": 2, "search_interval": 15})
        redis.eval = AsyncMock(side_effect=RedisError("NOSCRIPT"))

        await remove_device_from_cluster("d1", redis)
        # Should not raise — error handled gracefully


class TestConcurrentPenalizeDevice:
    """Verify that concurrent penalizations produce correct state."""

    async def test_sequential_penalize_triggers_reset(self):
        redis = _make_cluster_redis()
        redis._store["device_cluster:d1"] = json.dumps({"cluster_id": "c1", "status": "active"})
        redis._store["device_cluster:d2"] = json.dumps({"cluster_id": "c1", "status": "active"})
        redis._store["device_cluster:d3"] = json.dumps({"cluster_id": "c1", "status": "active"})
        redis._store["cluster:c1"] = json.dumps({"active_members": 3, "search_interval": 15})
        redis._sets_store["cluster_members:c1"] = {"d1", "d2", "d3"}

        await penalize_device_in_cluster("d1", redis)
        await penalize_device_in_cluster("d2", redis)

        # Two penalized, one still active
        cluster_data = json.loads(redis._store["cluster:c1"])
        assert cluster_data["active_members"] == 1

        d1_data = json.loads(redis._store["device_cluster:d1"])
        assert d1_data["status"] == "penalized"

        # Now penalize the last one — should reset all
        await penalize_device_in_cluster("d3", redis)

        cluster_data = json.loads(redis._store["cluster:c1"])
        assert cluster_data["active_members"] == 3

        for did in ("d1", "d2", "d3"):
            d_data = json.loads(redis._store[f"device_cluster:{did}"])
            assert d_data["status"] == "active"

    async def test_penalize_with_expired_cluster(self):
        redis = _make_cluster_redis()
        redis._store["device_cluster:d1"] = json.dumps({"cluster_id": "c1", "status": "active"})
        # cluster:c1 expired — not in store

        await penalize_device_in_cluster("d1", redis)

        # Device should still be marked penalized
        d1_data = json.loads(redis._store["device_cluster:d1"])
        assert d1_data["status"] == "penalized"

    async def test_penalize_redis_error_on_eval(self):
        redis = _make_cluster_redis()
        redis._store["device_cluster:d1"] = json.dumps({"cluster_id": "c1", "status": "active"})
        redis._store["cluster:c1"] = json.dumps({"active_members": 2, "search_interval": 15})
        redis.eval = AsyncMock(side_effect=RedisError("NOSCRIPT"))

        await penalize_device_in_cluster("d1", redis)
        # Should not raise — error handled gracefully
