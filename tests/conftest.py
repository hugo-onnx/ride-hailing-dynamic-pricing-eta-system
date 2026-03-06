"""Shared fixtures and path setup for all tests."""
import os
import sys
import pytest
import fakeredis

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
INFERENCE_API_DIR = os.path.join(ROOT, "services", "inference-api")

for _path in [ROOT, INFERENCE_API_DIR]:
    if _path not in sys.path:
        sys.path.insert(0, _path)


@pytest.fixture
def fake_redis():
    """In-memory Redis via fakeredis — no real Redis needed."""
    return fakeredis.FakeRedis(decode_responses=True)


@pytest.fixture
def sample_ride_event():
    """Minimal valid ride event dict."""
    return {
        "event_id": "test-event-1",
        "timestamp": "2024-06-15T10:15:00+00:00",
        "lat": 40.4169,
        "lng": -3.7034,
        "h3_res8": "881e2040c9fffff",
        "zone": "Sol-Gran Via",
    }


@pytest.fixture
def sample_driver_event():
    """Minimal valid driver event dict."""
    return {
        "driver_id": "d_00001",
        "timestamp": "2024-06-15T10:15:00+00:00",
        "lat": 40.4169,
        "lng": -3.7034,
        "h3_res8": "881e2040c9fffff",
        "status": "available",
        "idle_seconds": 60,
    }


@pytest.fixture
def populated_redis(fake_redis):
    """Redis pre-loaded with 5m window feature data for a known H3 cell."""
    from datetime import datetime, timezone, timedelta
    from features import floor_completed_window, redis_key

    ts = datetime.now(timezone.utc)
    h3_index = "881e2040c9fffff"

    for window in [1, 5, 15]:
        window_start = floor_completed_window(ts, window)
        key = redis_key("madrid", h3_index, window, window_start)
        fake_redis.hset(key, mapping={
            "ride_requests": "10",
            "idle_drivers": "15",
            "on_trip_drivers": "5",
            "deadhead_km_sum": "30.0",
            "idle_events": "15",
            "h3_res8": h3_index,
            "window_minutes": str(window),
            "window_start": window_start.isoformat(),
        })

    return fake_redis
