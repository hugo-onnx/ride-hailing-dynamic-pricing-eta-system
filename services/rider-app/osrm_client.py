"""
Async OSRM client for fetching driver approach routes with geometry.
"""

import os
import math
import logging

import httpx

logger = logging.getLogger(__name__)

OSRM_HOST = os.getenv("OSRM_HOST", "http://osrm:5000")


class AsyncOSRMClient:
    def __init__(self, host: str = OSRM_HOST):
        self.host = host.rstrip("/")
        self.client = httpx.AsyncClient(timeout=3.0)

    async def get_route(
        self,
        origin_lng: float,
        origin_lat: float,
        dest_lng: float,
        dest_lat: float,
    ) -> dict:
        """Get route with full GeoJSON geometry."""
        coords = f"{origin_lng},{origin_lat};{dest_lng},{dest_lat}"
        url = f"{self.host}/route/v1/driving/{coords}"

        try:
            resp = await self.client.get(url, params={
                "overview": "full",
                "geometries": "geojson",
                "annotations": "false",
            })
            resp.raise_for_status()
            data = resp.json()

            if data.get("code") != "Ok" or not data.get("routes"):
                return self._haversine_fallback(
                    origin_lng, origin_lat, dest_lng, dest_lat
                )

            route = data["routes"][0]
            return {
                "distance_m": route["distance"],
                "duration_s": route["duration"],
                "geometry": route["geometry"],
            }

        except Exception as e:
            logger.warning(f"OSRM request failed, using haversine fallback: {e}")
            return self._haversine_fallback(
                origin_lng, origin_lat, dest_lng, dest_lat
            )

    def _haversine_fallback(
        self,
        origin_lng: float,
        origin_lat: float,
        dest_lng: float,
        dest_lat: float,
    ) -> dict:
        dist_km = self._haversine_km(origin_lat, origin_lng, dest_lat, dest_lng)
        duration_s = (dist_km / 35) * 3600
        return {
            "distance_m": dist_km * 1000,
            "duration_s": duration_s,
            "geometry": {
                "type": "LineString",
                "coordinates": [
                    [origin_lng, origin_lat],
                    [dest_lng, dest_lat],
                ],
            },
        }

    @staticmethod
    def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        R = 6371.0
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        a = (
            math.sin(dlat / 2) ** 2
            + math.cos(math.radians(lat1))
            * math.cos(math.radians(lat2))
            * math.sin(dlon / 2) ** 2
        )
        return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    async def close(self):
        await self.client.aclose()
