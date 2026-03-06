"""Integration tests for the inference-api FastAPI app."""
import sys
import os
import importlib.util
import pytest
import fakeredis
from unittest.mock import MagicMock
from fastapi.testclient import TestClient

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
INFERENCE_API_DIR = os.path.join(ROOT, "services", "inference-api")

for _p in [ROOT, INFERENCE_API_DIR]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

# Load inference-api main with a unique module name
_MAIN_PATH = os.path.join(INFERENCE_API_DIR, "main.py")
if "inference_api_main" not in sys.modules:
    _spec = importlib.util.spec_from_file_location("inference_api_main", _MAIN_PATH)
    _mod = importlib.util.module_from_spec(_spec)
    sys.modules["inference_api_main"] = _mod
    _spec.loader.exec_module(_mod)

m = sys.modules["inference_api_main"]


def _make_pickup_model(return_value=300.0):
    model = MagicMock()
    model.predict.return_value = return_value
    return model


def _make_dropoff_model(return_value=600.0):
    model = MagicMock()
    model.predict.return_value = return_value
    return model


@pytest.fixture
def client_with_redis(fake_redis):
    """TestClient with fakeredis injected as the global redis_client."""
    m.redis_client = fake_redis
    m.pickup_model = None
    m.dropoff_model = None
    m.osrm_available = False
    yield TestClient(m.app)
    m.redis_client = None


@pytest.fixture
def client_with_models(fake_redis):
    """TestClient with fakeredis and mock ML models."""
    m.redis_client = fake_redis
    m.pickup_model = _make_pickup_model()
    m.dropoff_model = _make_dropoff_model()
    m.osrm_available = False
    yield TestClient(m.app)
    m.redis_client = None
    m.pickup_model = None
    m.dropoff_model = None


class TestHealthEndpoint:
    def test_health_ok(self, client_with_redis):
        resp = client_with_redis.get("/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

    def test_health_503_when_redis_none(self):
        m.redis_client = None
        client = TestClient(m.app)
        resp = client.get("/health")
        assert resp.status_code == 503

    def test_health_returns_osrm_status(self, client_with_redis):
        resp = client_with_redis.get("/health")
        data = resp.json()
        assert "osrm" in data


class TestFeaturesEndpoint:
    def test_features_endpoint_returns_valid_shape(self, client_with_redis):
        resp = client_with_redis.get("/v1/features?lat=40.4169&lng=-3.7034")
        assert resp.status_code == 200
        data = resp.json()
        assert "h3_res8" in data
        assert "features" in data
        assert "1m" in data["features"]
        assert "5m" in data["features"]
        assert "15m" in data["features"]

    def test_features_invalid_timestamp_returns_400(self, client_with_redis):
        resp = client_with_redis.get(
            "/v1/features?lat=40.4169&lng=-3.7034&timestamp=not-a-date"
        )
        assert resp.status_code == 400


class TestPricingQuoteEndpoint:
    def test_pricing_quote_returns_valid_shape(self, client_with_redis):
        resp = client_with_redis.post("/v1/pricing/quote?lat=40.4169&lng=-3.7034")
        assert resp.status_code == 200
        data = resp.json()
        assert "pricing" in data
        assert "multiplier" in data["pricing"]
        assert "surge_level" in data["pricing"]

    def test_pricing_quote_empty_features_dampened(self, client_with_redis):
        """With no Redis data (0 supply, 0 demand) → low-volume dampening applies."""
        resp = client_with_redis.post("/v1/pricing/quote?lat=40.4169&lng=-3.7034")
        assert resp.status_code == 200
        data = resp.json()
        # 0 rides < 5 → low-volume dampening; multiplier is 1.4 not 2.0
        assert data["pricing"]["multiplier"] < 2.0
        assert data["pricing"]["multiplier"] >= 1.0
        assert "low_volume_dampening" in data["pricing"]["reasons"]

    def test_pricing_quote_latency_ms_present(self, client_with_redis):
        resp = client_with_redis.post("/v1/pricing/quote?lat=40.4169&lng=-3.7034")
        assert "latency_ms" in resp.json()


class TestETAQuoteEndpoint:
    def test_eta_quote_503_when_model_not_loaded(self, client_with_redis):
        resp = client_with_redis.post(
            "/v1/eta/quote?lat=40.4169&lng=-3.7034&trip_distance_km=5.0"
        )
        assert resp.status_code == 503

    def test_eta_quote_returns_eta_seconds(self, client_with_models):
        resp = client_with_models.post(
            "/v1/eta/quote?lat=40.4169&lng=-3.7034&trip_distance_km=5.0"
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "eta_seconds" in data
        assert "eta_minutes" in data
        assert data["eta_seconds"] >= 120  # ETA_MIN_S


class TestRouteEndpoint:
    def test_route_endpoint_returns_haversine_fallback(self, client_with_redis):
        """With OSRM unavailable, haversine fallback is used."""
        resp = client_with_redis.get(
            "/v1/route?origin_lat=40.4169&origin_lng=-3.7034"
            "&dest_lat=40.4722&dest_lng=-3.6824"
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["source"] == "haversine_fallback"
        assert data["distance_km"] > 0

    def test_route_fallback_uses_35_kmh(self, client_with_redis):
        """duration_min = (distance_km / 35) * 60 for haversine fallback."""
        resp = client_with_redis.get(
            "/v1/route?origin_lat=40.4169&origin_lng=-3.7034"
            "&dest_lat=40.4722&dest_lng=-3.6824"
        )
        data = resp.json()
        expected_min = (data["distance_km"] / 35) * 60
        assert abs(data["duration_min"] - expected_min) < 0.1


class TestTripQuoteEndpoint:
    def test_trip_quote_503_when_pickup_model_not_loaded(self, client_with_redis):
        resp = client_with_redis.post(
            "/v1/quote?origin_lat=40.4169&origin_lng=-3.7034"
            "&dest_lat=40.4722&dest_lng=-3.6824"
        )
        assert resp.status_code == 503

    def test_trip_quote_503_when_dropoff_model_not_loaded(self, fake_redis):
        m.redis_client = fake_redis
        m.pickup_model = _make_pickup_model()
        m.dropoff_model = None
        m.osrm_available = False
        client = TestClient(m.app)
        resp = client.post(
            "/v1/quote?origin_lat=40.4169&origin_lng=-3.7034"
            "&dest_lat=40.4722&dest_lng=-3.6824"
        )
        assert resp.status_code == 503
        m.redis_client = None
        m.pickup_model = None

    def test_trip_quote_returns_complete_response(self, client_with_models):
        resp = client_with_models.post(
            "/v1/quote?origin_lat=40.4169&origin_lng=-3.7034"
            "&dest_lat=40.4722&dest_lng=-3.6824"
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "route" in data
        assert "eta" in data
        assert "price" in data
        assert data["eta"]["pickup_seconds"] >= 0
        assert data["price"]["multiplier"] >= 1.0

    def test_trip_quote_too_close_raises_400(self, client_with_models):
        """Same origin and destination → 400 (distance < 0.1 km)."""
        resp = client_with_models.post(
            "/v1/quote?origin_lat=40.4169&origin_lng=-3.7034"
            "&dest_lat=40.4169&dest_lng=-3.7034"
        )
        assert resp.status_code == 400
