import h3
from datetime import datetime, timezone

from fastapi import HTTPException, Request


def get_redis(request: Request):
    rc = request.app.state.redis_client
    if rc is None:
        raise HTTPException(status_code=503, detail="Redis not initialized")
    return rc


def get_pickup_model(request: Request):
    return request.app.state.pickup_model


def get_dropoff_model(request: Request):
    return request.app.state.dropoff_model


def get_osrm_available(request: Request) -> bool:
    return request.app.state.osrm_available


def parse_timestamp(timestamp: str | None) -> datetime:
    if timestamp:
        try:
            ts = datetime.fromisoformat(timestamp)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid timestamp format. Use ISO 8601.")
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return ts
    return datetime.now(timezone.utc)


def resolve_h3(lat: float, lng: float, resolution: int = 8) -> str:
    try:
        return h3.latlng_to_cell(lat, lng, resolution)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid coordinates: {e}")
