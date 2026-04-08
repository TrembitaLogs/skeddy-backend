"""Cluster service — device clustering via H3 hexagonal grid and Union-Find.

Handles cluster building, device removal/penalization, and search coordination
via an atomic Redis Lua script (cluster gate).
"""

import json
import logging
import math
import time
from collections import defaultdict
from typing import Any

import h3
from redis.asyncio import Redis
from redis.exceptions import RedisError

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Redis key templates & TTL
# ---------------------------------------------------------------------------

DEVICE_CLUSTER_KEY = "device_cluster:{device_id}"
CLUSTER_KEY = "cluster:{cluster_id}"
CLUSTER_MEMBERS_KEY = "cluster_members:{cluster_id}"
CLUSTER_LAST_SEARCH_KEY = "cluster_last_search:{cluster_id}"
CLUSTER_TTL = 420  # 7 minutes

# ---------------------------------------------------------------------------
# Lua script for atomic search coordination
# ---------------------------------------------------------------------------

CLUSTER_GATE_LUA = """
local key = KEYS[1]
local now = tonumber(ARGV[1])
local interval = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])

local last = redis.call('GET', key)
if last == false or (now - tonumber(last)) >= interval then
    redis.call('SET', key, tostring(now))
    redis.call('EXPIRE', key, ttl)
    return 1
else
    return math.ceil(interval - (now - tonumber(last)))
end
"""

# ---------------------------------------------------------------------------
# Haversine distance (miles)
# ---------------------------------------------------------------------------

_EARTH_RADIUS_MILES = 3958.8


def haversine_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Return the great-circle distance in miles between two lat/lon points."""
    lat1_r, lon1_r = math.radians(lat1), math.radians(lon1)
    lat2_r, lon2_r = math.radians(lat2), math.radians(lon2)
    dlat = lat2_r - lat1_r
    dlon = lon2_r - lon1_r
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1_r) * math.cos(lat2_r) * math.sin(dlon / 2) ** 2
    return _EARTH_RADIUS_MILES * 2 * math.asin(math.sqrt(a))


def _min_distance_between(
    devices_a: list[dict[str, Any]], devices_b: list[dict[str, Any]]
) -> float:
    """Return the minimum pairwise haversine distance (miles) between two device groups."""
    min_dist = float("inf")
    for da in devices_a:
        for db in devices_b:
            dist = haversine_miles(da["lat"], da["lon"], db["lat"], db["lon"])
            if dist < min_dist:
                min_dist = dist
    return min_dist


# ---------------------------------------------------------------------------
# Union-Find
# ---------------------------------------------------------------------------


class UnionFind:
    """Standard Union-Find with path compression and union by rank."""

    def __init__(self, elements: Any) -> None:
        self.parent: dict[str, str] = {e: e for e in elements}
        self.rank: dict[str, int] = {e: 0 for e in elements}

    def find(self, x: str) -> str:
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]  # path compression
            x = self.parent[x]
        return x

    def union(self, x: str, y: str) -> None:
        rx, ry = self.find(x), self.find(y)
        if rx == ry:
            return
        if self.rank[rx] < self.rank[ry]:
            rx, ry = ry, rx
        self.parent[ry] = rx
        if self.rank[rx] == self.rank[ry]:
            self.rank[rx] += 1


# ---------------------------------------------------------------------------
# build_clusters
# ---------------------------------------------------------------------------


async def build_clusters(
    devices: list[dict[str, Any]],
    threshold_miles: int,
    redis: Redis,
) -> dict[str, list[dict[str, Any]]]:
    """Build clusters from eligible devices using H3 cells and Union-Find.

    Each device dict must have keys: ``device_id``, ``lat``, ``lon``.
    Solo devices (cluster size 1) are excluded from the result and not written
    to Redis.

    Returns ``{cluster_id: [device, ...]}`` for clusters with 2+ members.
    """
    if not devices:
        return {}

    # Step 3a: distribute devices into H3 cells (resolution 5)
    cells: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for device in devices:
        cell = h3.latlng_to_cell(device["lat"], device["lon"], res=5)
        cells[cell].append(device)

    # Step 3b: Union-Find for neighbouring occupied cells within threshold
    uf = UnionFind(cells.keys())
    cell_list = list(cells.keys())
    for cell in cell_list:
        for neighbor in h3.grid_ring(cell, 1):
            if (
                neighbor in cells
                and _min_distance_between(cells[cell], cells[neighbor]) <= threshold_miles
            ):
                uf.union(cell, neighbor)

    # Group devices by root cell
    clusters: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for cell, devs in cells.items():
        root = uf.find(cell)
        clusters[root].extend(devs)

    # Filter out solo devices
    return {cid: members for cid, members in clusters.items() if len(members) >= 2}


# ---------------------------------------------------------------------------
# Write clusters to Redis (Step 6 from PRD)
# ---------------------------------------------------------------------------


async def write_clusters_to_redis(
    clusters: dict[str, list[dict[str, Any]]],
    device_statuses: dict[str, str],
    cluster_params: dict[str, dict[str, Any]],
    redis: Redis,
) -> None:
    """Write cluster data to Redis with TTL.

    Args:
        clusters: cluster_id -> list of device dicts
        device_statuses: device_id -> "active" | "penalized"
        cluster_params: cluster_id -> {"active_members": N, "search_interval": M}
    """
    pipe = redis.pipeline()
    for cluster_id, members in clusters.items():
        params = cluster_params.get(cluster_id, {})
        # cluster:{cluster_id}
        cluster_key = CLUSTER_KEY.format(cluster_id=cluster_id)
        pipe.setex(cluster_key, CLUSTER_TTL, json.dumps(params))

        # cluster_members:{cluster_id}
        members_key = CLUSTER_MEMBERS_KEY.format(cluster_id=cluster_id)
        pipe.delete(members_key)
        pipe.sadd(members_key, *[d["device_id"] for d in members])
        pipe.expire(members_key, CLUSTER_TTL)

        # device_cluster:{device_id} for each member
        for device in members:
            did = device["device_id"]
            status = device_statuses.get(did, "active")
            dc_key = DEVICE_CLUSTER_KEY.format(device_id=did)
            pipe.setex(
                dc_key,
                CLUSTER_TTL,
                json.dumps({"cluster_id": cluster_id, "status": status}),
            )

    await pipe.execute()


# ---------------------------------------------------------------------------
# remove_device_from_cluster
# ---------------------------------------------------------------------------


async def remove_device_from_cluster(device_id: str, redis: Redis) -> None:
    """Remove a device from its cluster in Redis.

    If the device is solo (no cluster key), this is a no-op.
    If the device was the last member, cleans up all cluster keys.
    """
    dc_key = DEVICE_CLUSTER_KEY.format(device_id=device_id)
    try:
        raw = await redis.get(dc_key)
    except RedisError:
        logger.warning("Redis unavailable when reading %s", dc_key)
        return

    if raw is None:
        return  # solo device

    try:
        data = json.loads(raw)
        cluster_id = data["cluster_id"]
    except (json.JSONDecodeError, KeyError):
        logger.warning("Invalid device_cluster data for %s: %r", device_id, raw)
        return

    cluster_key = CLUSTER_KEY.format(cluster_id=cluster_id)
    members_key = CLUSTER_MEMBERS_KEY.format(cluster_id=cluster_id)
    last_search_key = CLUSTER_LAST_SEARCH_KEY.format(cluster_id=cluster_id)

    try:
        await redis.delete(dc_key)
        await redis.srem(members_key, device_id)

        # Decrement active_members
        cluster_raw = await redis.get(cluster_key)
        if cluster_raw is not None:
            cluster_data = json.loads(cluster_raw)
            cluster_data["active_members"] = max(0, cluster_data.get("active_members", 1) - 1)

            if cluster_data["active_members"] <= 0:
                # Last member — clean up cluster keys
                await redis.delete(cluster_key, members_key, last_search_key)
            else:
                ttl = await redis.ttl(cluster_key)
                if ttl > 0:
                    await redis.setex(cluster_key, ttl, json.dumps(cluster_data))
                else:
                    await redis.setex(cluster_key, CLUSTER_TTL, json.dumps(cluster_data))
    except RedisError:
        logger.warning("Redis error during remove_device_from_cluster for %s", device_id)


# ---------------------------------------------------------------------------
# penalize_device_in_cluster
# ---------------------------------------------------------------------------


async def penalize_device_in_cluster(device_id: str, redis: Redis) -> None:
    """Mark a device as penalized within its cluster.

    If all members become penalized, reset all penalties (everyone becomes active).
    """
    dc_key = DEVICE_CLUSTER_KEY.format(device_id=device_id)
    try:
        raw = await redis.get(dc_key)
    except RedisError:
        logger.warning("Redis unavailable when reading %s", dc_key)
        return

    if raw is None:
        return  # solo device

    try:
        data = json.loads(raw)
        cluster_id = data["cluster_id"]
    except (json.JSONDecodeError, KeyError):
        logger.warning("Invalid device_cluster data for %s: %r", device_id, raw)
        return

    cluster_key = CLUSTER_KEY.format(cluster_id=cluster_id)
    members_key = CLUSTER_MEMBERS_KEY.format(cluster_id=cluster_id)

    try:
        # Update device status to penalized
        data["status"] = "penalized"
        ttl = await redis.ttl(dc_key)
        if ttl > 0:
            await redis.setex(dc_key, ttl, json.dumps(data))
        else:
            await redis.setex(dc_key, CLUSTER_TTL, json.dumps(data))

        # Decrement active_members in cluster
        cluster_raw = await redis.get(cluster_key)
        if cluster_raw is None:
            return

        cluster_data = json.loads(cluster_raw)
        cluster_data["active_members"] = max(0, cluster_data.get("active_members", 1) - 1)

        if cluster_data["active_members"] <= 0:
            # All penalized — reset everyone to active
            member_ids = await redis.smembers(members_key)
            total_members = len(member_ids) if member_ids else 0

            for mid in member_ids or []:
                mid_key = DEVICE_CLUSTER_KEY.format(device_id=mid)
                mid_raw = await redis.get(mid_key)
                if mid_raw is not None:
                    mid_data = json.loads(mid_raw)
                    mid_data["status"] = "active"
                    mid_ttl = await redis.ttl(mid_key)
                    if mid_ttl > 0:
                        await redis.setex(mid_key, mid_ttl, json.dumps(mid_data))
                    else:
                        await redis.setex(mid_key, CLUSTER_TTL, json.dumps(mid_data))

            cluster_data["active_members"] = total_members

        cluster_ttl = await redis.ttl(cluster_key)
        if cluster_ttl > 0:
            await redis.setex(cluster_key, cluster_ttl, json.dumps(cluster_data))
        else:
            await redis.setex(cluster_key, CLUSTER_TTL, json.dumps(cluster_data))

    except RedisError:
        logger.warning("Redis error during penalize_device_in_cluster for %s", device_id)


# ---------------------------------------------------------------------------
# cluster_gate
# ---------------------------------------------------------------------------


async def cluster_gate(
    device_id: str,
    redis: Redis,
    clustering_enabled: bool,
) -> dict[str, Any] | None:
    """Determine whether this device should search or wait.

    Returns:
        None — skip clustering, device operates as solo (existing logic applies).
        {"search": True,  "interval_seconds": N} — device should search now.
        {"search": False, "interval_seconds": N} — device should wait N seconds.
    """
    if not clustering_enabled:
        return None

    dc_key = DEVICE_CLUSTER_KEY.format(device_id=device_id)
    try:
        raw = await redis.get(dc_key)
    except RedisError:
        logger.warning("Redis error in cluster_gate for %s, treating as solo", device_id)
        return None

    if raw is None:
        return None  # solo device

    try:
        data = json.loads(raw)
        cluster_id = data["cluster_id"]
        status = data.get("status", "active")
    except (json.JSONDecodeError, KeyError):
        logger.warning("Invalid device_cluster data in cluster_gate for %s", device_id)
        return None

    # Penalized device — no search, short retry interval
    if status == "penalized":
        return {"search": False, "interval_seconds": 60}

    # Active device — use Lua script for atomic coordination
    cluster_key = CLUSTER_KEY.format(cluster_id=cluster_id)
    last_search_key = CLUSTER_LAST_SEARCH_KEY.format(cluster_id=cluster_id)

    try:
        cluster_raw = await redis.get(cluster_key)
        if cluster_raw is None:
            return None  # cluster expired

        cluster_data = json.loads(cluster_raw)
        active_members = cluster_data.get("active_members", 1)
        search_interval = cluster_data.get("search_interval", 15)

        now = time.time()
        result = await redis.eval(
            CLUSTER_GATE_LUA,
            1,
            last_search_key,
            str(now),
            str(search_interval),
            str(CLUSTER_TTL),
        )

        if result == 1:
            # This device wins the search slot
            return {
                "search": True,
                "interval_seconds": search_interval * active_members,
            }
        else:
            # Wait — result is remaining seconds
            return {"search": False, "interval_seconds": int(result)}

    except RedisError:
        logger.warning("Redis error in cluster_gate Lua for %s, treating as solo", device_id)
        return None
