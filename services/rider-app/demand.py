"""
Demand data aggregator — reads H3 hexagon demand from Redis for heatmap display.
"""

import logging
from datetime import datetime, timezone

import redis.asyncio as redis

from services.common.config import CITY
from services.common.time_utils import floor_timestamp

logger = logging.getLogger(__name__)


class DemandAggregator:
    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client

    async def get_current_demand(self, window_minutes: int = 5) -> list[dict]:
        now = datetime.now(timezone.utc)
        window_start = floor_timestamp(now, window_minutes)
        pattern = f"{CITY}:*:{window_minutes}m:{window_start.isoformat()}"

        hexagons = []

        try:
            cursor = 0
            keys = []
            while True:
                cursor, batch = await self.redis.scan(cursor, match=pattern, count=1000)
                keys.extend(batch)
                if cursor == 0:
                    break

            if not keys:
                return hexagons

            pipe = self.redis.pipeline()
            for key in keys:
                pipe.hgetall(key)
            results = await pipe.execute()

            for key, data in zip(keys, results):
                if not data:
                    continue

                h3_index = data.get("h3_res8", "")
                if not h3_index:
                    parts = key.split(":")
                    if len(parts) >= 2:
                        h3_index = parts[1]

                ride_requests = int(data.get("ride_requests", 0))
                idle_drivers = int(data.get("idle_drivers", 0))
                on_trip_drivers = int(data.get("on_trip_drivers", 0))

                hexagons.append({
                    "h3_index": h3_index,
                    "ride_requests": ride_requests,
                    "idle_drivers": idle_drivers,
                    "on_trip_drivers": on_trip_drivers,
                    "demand_ratio": ride_requests,
                })

            return hexagons

        except Exception as e:
            logger.error(f"Error fetching demand data: {e}")
            return []
