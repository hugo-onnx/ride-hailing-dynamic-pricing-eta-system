"""Integration tests for the visualization FastAPI app."""
import sys
import os
import importlib.util
import pytest
import fakeredis
import fakeredis.aioredis
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi.testclient import TestClient

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

_VIZ_PATH = os.path.join(ROOT, "services", "visualization", "main.py")
if "visualization_main" not in sys.modules:
    _spec = importlib.util.spec_from_file_location("visualization_main", _VIZ_PATH)
    _mod = importlib.util.module_from_spec(_spec)
    sys.modules["visualization_main"] = _mod
    _spec.loader.exec_module(_mod)

viz = sys.modules["visualization_main"]


@pytest.fixture
def async_fake_redis():
    return fakeredis.aioredis.FakeRedis(decode_responses=True)


@pytest.fixture
def viz_client(async_fake_redis):
    """TestClient with async fakeredis injected into the visualization app."""
    # Use the already-loaded module to avoid re-importing (which would re-register Prometheus metrics)
    DemandAggregator = viz.DemandAggregator
    viz.redis_client = async_fake_redis
    viz.aggregator = DemandAggregator(async_fake_redis)
    yield TestClient(viz.app)
    viz.redis_client = None
    viz.aggregator = None


class TestHealthEndpoint:
    def test_health_ok(self, viz_client):
        resp = viz_client.get("/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "healthy"


class TestDemandEndpoint:
    def test_demand_endpoint_returns_hexagons_key(self, viz_client):
        resp = viz_client.get("/api/demand?window=5")
        assert resp.status_code == 200
        data = resp.json()
        assert "hexagons" in data
        assert "stats" in data
        assert isinstance(data["hexagons"], list)

    def test_demand_endpoint_returns_timestamp(self, viz_client):
        resp = viz_client.get("/api/demand")
        assert "timestamp" in resp.json()

    def test_demand_invalid_window_returns_422(self, viz_client):
        resp = viz_client.get("/api/demand?window=100")
        assert resp.status_code == 422


class TestStatsEndpoint:
    def test_stats_endpoint_returns_counts(self, viz_client):
        resp = viz_client.get("/api/stats?window=5")
        assert resp.status_code == 200
        data = resp.json()
        assert "total_hexagons" in data
        assert "total_ride_requests" in data
        assert "total_idle_drivers" in data


class TestWebSocketEndpoint:
    def test_websocket_connect_and_receive_initial(self, viz_client):
        with viz_client.websocket_connect("/ws") as ws:
            data = ws.receive_json()
            assert data["type"] == "initial"
            assert "hexagons" in data

    def test_websocket_ping_pong(self, viz_client):
        with viz_client.websocket_connect("/ws") as ws:
            ws.receive_json()  # consume initial message
            ws.send_json({"type": "ping"})
            pong = ws.receive_json()
            assert pong["type"] == "pong"

    def test_websocket_invalid_json_does_not_crash(self, viz_client):
        """Sending invalid JSON should not crash the handler."""
        with viz_client.websocket_connect("/ws") as ws:
            ws.receive_json()  # consume initial
            ws.send_text("not valid json {{{{")
            # After invalid JSON, the connection should still be alive
            # Send a valid message to confirm
            ws.send_json({"type": "ping"})
            pong = ws.receive_json()
            assert pong["type"] == "pong"

    def test_websocket_request_update(self, viz_client):
        with viz_client.websocket_connect("/ws") as ws:
            ws.receive_json()  # consume initial
            ws.send_json({"type": "request_update", "window": 1})
            data = ws.receive_json()
            assert data["type"] == "demand_update"
            assert data["window_minutes"] == 1
