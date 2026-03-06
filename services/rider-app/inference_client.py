"""
Async client for the inference API service.
"""

import os
import logging

import httpx

logger = logging.getLogger(__name__)

INFERENCE_API_URL = os.getenv("INFERENCE_API_URL", "http://inference-api:8000")


class InferenceClient:
    def __init__(self, base_url: str = INFERENCE_API_URL):
        self.base_url = base_url.rstrip("/")
        self.client = httpx.AsyncClient(base_url=self.base_url, timeout=10.0)

    async def get_quote(
        self,
        origin_lat: float,
        origin_lng: float,
        dest_lat: float,
        dest_lng: float,
    ) -> dict:
        resp = await self.client.post(
            "/v1/quote",
            params={
                "origin_lat": origin_lat,
                "origin_lng": origin_lng,
                "dest_lat": dest_lat,
                "dest_lng": dest_lng,
            },
        )
        resp.raise_for_status()
        return resp.json()

    async def close(self):
        await self.client.aclose()
