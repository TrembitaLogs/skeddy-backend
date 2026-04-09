"""Background task: rebuild device clusters every N minutes.

Reads eligible devices from the database, groups them into geographic
clusters via ``build_clusters`` (H3 + Union-Find), computes penalty
statuses and search intervals, then writes the result to Redis.

New cluster data is written *before* stale keys are removed so that
there is never a window where valid cluster state is absent.
"""

import asyncio
import logging
from datetime import UTC, datetime, timedelta

from redis.exceptions import RedisError
from sqlalchemy import select
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import AsyncSessionLocal
from app.models.app_config import AppConfig
from app.models.credit_balance import CreditBalance
from app.models.paired_device import PairedDevice
from app.models.ride import Ride
from app.models.search_status import SearchStatus
from app.redis import redis_client
from app.services.cluster_service import (
    build_clusters,
    write_clusters_to_redis,
)
from app.services.config_service import get_search_interval_config
from app.services.ping_service import calculate_dynamic_interval

logger = logging.getLogger(__name__)

# Defaults for clustering AppConfig keys
DEFAULT_CLUSTERING_ENABLED = False
DEFAULT_CLUSTERING_PENALTY_MINUTES = 60
DEFAULT_CLUSTERING_THRESHOLD_MILES = 16
DEFAULT_CLUSTERING_REBUILD_INTERVAL_MINUTES = 5

# Redis scan patterns for cleanup
_CLUSTER_KEY_PATTERNS = [
    "device_cluster:*",
    "cluster:*",
    "cluster_members:*",
    "cluster_last_search:*",
]

INITIAL_DELAY_SECONDS = 10


def _safe_int(value: str | int, default: int) -> int:
    """Parse *value* as an integer, returning *default* on failure."""
    try:
        return int(value)
    except (ValueError, TypeError):
        logger.warning("Invalid int config value %r, using default %d", value, default)
        return default


async def get_clustering_config(
    db: AsyncSession,
) -> dict[str, bool | int]:
    """Load clustering config keys from AppConfig in a single query.

    Returns a dict with keys: enabled, penalty_minutes, threshold_miles,
    rebuild_interval_minutes.
    """
    keys = [
        "clustering_enabled",
        "clustering_penalty_minutes",
        "clustering_threshold_miles",
        "clustering_rebuild_interval_minutes",
    ]
    result = await db.execute(
        select(AppConfig.key, AppConfig.value).where(AppConfig.key.in_(keys))
    )
    rows = {row.key: row.value for row in result.all()}

    enabled_raw = rows.get("clustering_enabled", "false")
    enabled = enabled_raw.lower() in ("true", "1", "yes")

    return {
        "enabled": enabled,
        "penalty_minutes": _safe_int(
            rows.get("clustering_penalty_minutes", DEFAULT_CLUSTERING_PENALTY_MINUTES),
            DEFAULT_CLUSTERING_PENALTY_MINUTES,
        ),
        "threshold_miles": _safe_int(
            rows.get("clustering_threshold_miles", DEFAULT_CLUSTERING_THRESHOLD_MILES),
            DEFAULT_CLUSTERING_THRESHOLD_MILES,
        ),
        "rebuild_interval_minutes": _safe_int(
            rows.get(
                "clustering_rebuild_interval_minutes",
                DEFAULT_CLUSTERING_REBUILD_INTERVAL_MINUTES,
            ),
            DEFAULT_CLUSTERING_REBUILD_INTERVAL_MINUTES,
        ),
    }


async def get_eligible_devices(db: AsyncSession) -> list[PairedDevice]:
    """Return devices eligible for clustering.

    Eligible criteria:
    - search_status.is_active = True
    - credit_balance.balance > 0
    - paired_device.offline_notified = False
    - paired_device.latitude IS NOT NULL
    """
    stmt = (
        select(PairedDevice)
        .join(SearchStatus, PairedDevice.user_id == SearchStatus.user_id)
        .join(CreditBalance, PairedDevice.user_id == CreditBalance.user_id)
        .where(
            SearchStatus.is_active.is_(True),
            CreditBalance.balance > 0,
            PairedDevice.offline_notified.is_(False),
            PairedDevice.latitude.isnot(None),
        )
    )
    result = await db.execute(stmt)
    return list(result.scalars().all())


def devices_to_dicts(devices: list[PairedDevice]) -> list[dict]:
    """Convert PairedDevice model instances to dicts for build_clusters."""
    return [
        {
            "device_id": d.device_id,
            "lat": d.latitude,
            "lon": d.longitude,
            "user_id": d.user_id,
        }
        for d in devices
    ]


async def compute_member_statuses(
    db: AsyncSession,
    cluster_devices: list[dict],
    penalty_minutes: int,
    now_utc: datetime,
) -> dict[str, str]:
    """Compute penalty status for each device in a cluster.

    For each device, find the most recent ride with event_type
    ACCEPTED or CONFIRMED. If that ride was created within
    penalty_minutes, the device is "penalized"; otherwise "active".

    If ALL members are penalized, reset all to "active".

    Returns dict mapping device_id to status string.
    """
    user_ids = [d["user_id"] for d in cluster_devices]

    # Find most recent accepted/confirmed ride per user in one query
    latest_ride_subq = (
        select(
            Ride.user_id,
            Ride.created_at,
        )
        .where(
            Ride.user_id.in_(user_ids),
            Ride.event_type.in_(["ACCEPTED", "CONFIRMED"]),
        )
        .distinct(Ride.user_id)
        .order_by(Ride.user_id, Ride.created_at.desc())
        .subquery()
    )

    result = await db.execute(select(latest_ride_subq))
    user_ride_map = {row.user_id: row.created_at for row in result.all()}

    penalty_threshold = now_utc - timedelta(minutes=penalty_minutes)

    statuses: dict[str, str] = {}
    for device in cluster_devices:
        ride_time = user_ride_map.get(device["user_id"])
        if ride_time is not None and ride_time > penalty_threshold:
            statuses[device["device_id"]] = "penalized"
        else:
            statuses[device["device_id"]] = "active"

    # If ALL members are penalized, reset all to active
    if statuses and all(s == "penalized" for s in statuses.values()):
        for did in statuses:
            statuses[did] = "active"

    return statuses


async def clear_cluster_keys(redis) -> None:
    """Delete all cluster-related Redis keys using SCAN."""
    for pattern in _CLUSTER_KEY_PATTERNS:
        cursor = 0
        while True:
            cursor, keys = await redis.scan(cursor=cursor, match=pattern, count=200)
            if keys:
                await redis.delete(*keys)
            if cursor == 0:
                break


async def cleanup_stale_cluster_keys(
    redis,
    current_cluster_ids: set[str],
    current_device_ids: set[str],
) -> None:
    """Remove cluster keys that are no longer part of the active set.

    Unlike ``clear_cluster_keys`` this only deletes *stale* keys so
    there is never a window where valid cluster data is missing.
    """
    keep_keys: set[str] = set()
    for cid in current_cluster_ids:
        keep_keys.add(f"device_cluster:{cid}")  # won't match, just placeholder
        keep_keys.add(f"cluster:{cid}")
        keep_keys.add(f"cluster_members:{cid}")
        keep_keys.add(f"cluster_last_search:{cid}")
    for did in current_device_ids:
        keep_keys.add(f"device_cluster:{did}")

    for pattern in _CLUSTER_KEY_PATTERNS:
        cursor = 0
        while True:
            cursor, keys = await redis.scan(cursor=cursor, match=pattern, count=200)
            stale = [k for k in keys if k not in keep_keys]
            if stale:
                await redis.delete(*stale)
            if cursor == 0:
                break


async def run_cluster_manager() -> None:
    """Background task: rebuild device clusters every N minutes.

    Infinite loop that:
    1. Checks clustering_enabled feature flag
    2. Fetches eligible devices from DB
    3. Builds clusters via H3 algorithm
    4. Computes penalty statuses
    5. Calculates search intervals
    6. Writes results to Redis
    7. Cleans up stale cluster keys (no gap — new data written first)
    8. Sleeps for rebuild_interval_minutes
    """
    logger.info("Cluster manager task started")
    await asyncio.sleep(INITIAL_DELAY_SECONDS)

    sleep_seconds = DEFAULT_CLUSTERING_REBUILD_INTERVAL_MINUTES * 60

    while True:
        try:
            async with AsyncSessionLocal() as db:
                # Step 1: Read config
                config = await get_clustering_config(db)
                sleep_seconds = config["rebuild_interval_minutes"] * 60

                if not config["enabled"]:
                    logger.debug("Clustering disabled, skipping cycle")
                    await asyncio.sleep(sleep_seconds)
                    continue

                # Step 2: Fetch eligible devices
                devices = await get_eligible_devices(db)
                logger.debug("Cluster manager: %d eligible device(s)", len(devices))

                if not devices:
                    # No eligible devices — clear all cluster state
                    await clear_cluster_keys(redis_client)
                    await asyncio.sleep(sleep_seconds)
                    continue

                # Step 3: Build clusters
                device_dicts = devices_to_dicts(devices)
                clusters = await build_clusters(
                    device_dicts,
                    config["threshold_miles"],
                    redis_client,
                )

                if not clusters:
                    logger.debug("Cluster manager: no multi-device clusters formed")
                    # No clusters — clear all cluster state
                    await clear_cluster_keys(redis_client)
                    await asyncio.sleep(sleep_seconds)
                    continue

                # Step 4: Compute penalty statuses per cluster
                now_utc = datetime.now(UTC)
                all_device_statuses: dict[str, str] = {}
                cluster_params: dict[str, dict] = {}

                # Get search interval config for dynamic interval calculation
                interval_config = await get_search_interval_config(db, redis_client)

                for cluster_id, cluster_devs in clusters.items():
                    statuses = await compute_member_statuses(
                        db,
                        cluster_devs,
                        config["penalty_minutes"],
                        now_utc,
                    )
                    all_device_statuses.update(statuses)

                    # Step 5: Calculate search interval for this cluster
                    active_count = sum(1 for s in statuses.values() if s == "active")

                    if interval_config is not None and active_count > 0:
                        rpd, rph = interval_config
                        current_hour = datetime.now(UTC).hour
                        base_interval = calculate_dynamic_interval(rpd, rph, current_hour)
                        search_interval = base_interval
                    else:
                        search_interval = 60

                    cluster_params[cluster_id] = {
                        "active_members": active_count,
                        "search_interval": search_interval,
                    }

                # Step 6: Write new clusters to Redis (before cleanup)
                await write_clusters_to_redis(
                    clusters,
                    all_device_statuses,
                    cluster_params,
                    redis_client,
                )

                # Step 7: Remove stale keys that no longer belong to active clusters
                current_cluster_ids = set(clusters.keys())
                current_device_ids = {d["device_id"] for devs in clusters.values() for d in devs}
                await cleanup_stale_cluster_keys(
                    redis_client, current_cluster_ids, current_device_ids
                )

                logger.info(
                    "Cluster manager: rebuilt %d cluster(s) from %d device(s)",
                    len(clusters),
                    len(devices),
                )

        except (OperationalError, RedisError):
            logger.exception("Cluster manager cycle error")

        await asyncio.sleep(sleep_seconds)
