"""Cluster map view — interactive Google Maps visualization of device clusters."""

import json
import logging
import os

from sqladmin import BaseView, expose
from sqlalchemy import func, select
from starlette.responses import JSONResponse

from app.database import AsyncSessionLocal
from app.models.app_config import AppConfig
from app.models.credit_balance import CreditBalance
from app.models.paired_device import PairedDevice
from app.models.ride import Ride
from app.models.user import User
from app.redis import redis_client

logger = logging.getLogger(__name__)


async def _fetch_cluster_data() -> dict:
    """Build the cluster visualization payload from Redis and DB."""
    redis = redis_client

    async with AsyncSessionLocal() as db:
        # 1. Load clustering config from AppConfig
        result = await db.execute(
            select(AppConfig.key, AppConfig.value).where(
                AppConfig.key.in_(["clustering_enabled", "clustering_threshold_miles"])
            )
        )
        config_map = {r.key: r.value for r in result.all()}
        clustering_enabled = config_map.get("clustering_enabled", "false").lower() in (
            "true",
            "1",
            "yes",
        )
        threshold_miles = int(config_map.get("clustering_threshold_miles", "16"))

        # 2. Scan Redis for cluster:* keys
        cluster_ids: list[str] = []
        cursor: int = 0
        while True:
            cursor, keys = await redis.scan(cursor=cursor, match="cluster:*", count=200)
            for key in keys:
                cluster_ids.append(key.split(":", 1)[1])
            if cursor == 0:
                break

        # 3. Load cluster metadata, members, and device statuses from Redis
        clusters_redis: dict[str, dict] = {}
        clustered_device_ids: set[str] = set()
        device_statuses: dict[str, str] = {}

        for cid in cluster_ids:
            cluster_raw = await redis.get(f"cluster:{cid}")
            if cluster_raw is None:
                continue
            cluster_info = json.loads(cluster_raw)

            member_ids = await redis.smembers(f"cluster_members:{cid}")
            if not member_ids:
                continue

            for did in member_ids:
                dc_raw = await redis.get(f"device_cluster:{did}")
                if dc_raw:
                    device_statuses[did] = json.loads(dc_raw).get("status", "active")
                else:
                    device_statuses[did] = "active"
                clustered_device_ids.add(did)

            clusters_redis[cid] = {"info": cluster_info, "member_ids": member_ids}

        # 4. Load device details from DB (devices with location)
        last_ride_subq = (
            select(
                Ride.user_id,
                func.max(Ride.created_at).label("last_ride_at"),
            )
            .group_by(Ride.user_id)
            .subquery()
        )

        stmt = (
            select(
                PairedDevice.device_id,
                PairedDevice.latitude,
                PairedDevice.longitude,
                PairedDevice.last_ping_at,
                User.email,
                CreditBalance.balance,
                last_ride_subq.c.last_ride_at,
            )
            .join(User, PairedDevice.user_id == User.id)
            .outerjoin(CreditBalance, PairedDevice.user_id == CreditBalance.user_id)
            .outerjoin(last_ride_subq, PairedDevice.user_id == last_ride_subq.c.user_id)
            .where(PairedDevice.latitude.isnot(None))
        )
        rows = (await db.execute(stmt)).all()

    # 5. Build device lookup
    device_map: dict[str, dict] = {}
    for r in rows:
        device_map[r.device_id] = {
            "device_id": r.device_id,
            "user_name": r.email,
            "latitude": r.latitude,
            "longitude": r.longitude,
            "balance": r.balance or 0,
            "last_ping_at": r.last_ping_at.isoformat() if r.last_ping_at else None,
            "last_ride_at": r.last_ride_at.isoformat() if r.last_ride_at else None,
        }

    # 6. Build cluster response
    clusters: list[dict] = []
    for cid, cdata in clusters_redis.items():
        devices: list[dict] = []
        lats: list[float] = []
        lons: list[float] = []
        for did in cdata["member_ids"]:
            dev = device_map.get(did)
            if dev:
                devices.append({**dev, "status": device_statuses.get(did, "active")})
                lats.append(dev["latitude"])
                lons.append(dev["longitude"])
        if not devices:
            continue
        clusters.append(
            {
                "cluster_id": cid,
                "active_members": cdata["info"].get("active_members", 0),
                "total_members": len(devices),
                "search_interval": cdata["info"].get("search_interval", 60),
                "centroid": {"lat": sum(lats) / len(lats), "lon": sum(lons) / len(lons)},
                "devices": devices,
            }
        )

    # 7. Solo devices (have location but not in any cluster)
    solo_devices = [
        {
            "device_id": d["device_id"],
            "user_name": d["user_name"],
            "latitude": d["latitude"],
            "longitude": d["longitude"],
            "balance": d["balance"],
            "last_ping_at": d["last_ping_at"],
        }
        for did, d in device_map.items()
        if did not in clustered_device_ids
    ]

    return {
        "clustering_enabled": clustering_enabled,
        "clustering_threshold_miles": threshold_miles,
        "clusters": clusters,
        "solo_devices": solo_devices,
    }


class ClusterMapView(BaseView):
    """Cluster map view with Google Maps visualization."""

    name = "Cluster Map"
    icon = "fa-solid fa-map-location-dot"

    @expose("/cluster-map", methods=["GET"])
    async def cluster_map(self, request):
        """Render the cluster map page."""
        maps_api_key = os.environ.get("GOOGLE_MAPS_API_KEY", "")
        if not maps_api_key:
            async with AsyncSessionLocal() as db:
                result = await db.execute(
                    select(AppConfig.value).where(AppConfig.key == "google_maps_api_key")
                )
                maps_api_key = result.scalar_one_or_none() or ""

        return await self.templates.TemplateResponse(
            request,
            "admin/cluster_map.html",
            {"maps_api_key": maps_api_key},
        )

    @expose("/api/clusters", methods=["GET"])
    async def data_clusters_api(self, request):
        """Return cluster data as JSON for map visualization."""
        try:
            data = await _fetch_cluster_data()
            return JSONResponse(content=data)
        except Exception:
            logger.exception("Failed to build cluster data")
            return JSONResponse(
                content={"error": "Failed to load cluster data"},
                status_code=500,
            )
