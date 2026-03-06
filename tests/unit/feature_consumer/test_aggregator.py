"""Tests for FeatureAggregator in services/feature-consumer/main.py"""
import sys
import os
import importlib.util
import time
import pytest
import fakeredis

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

# Load feature-consumer main with a unique module name to avoid collision
_FC_PATH = os.path.join(ROOT, "services", "feature-consumer", "main.py")
if "feature_consumer_main" not in sys.modules:
    _spec = importlib.util.spec_from_file_location("feature_consumer_main", _FC_PATH)
    _mod = importlib.util.module_from_spec(_spec)
    sys.modules["feature_consumer_main"] = _mod
    _spec.loader.exec_module(_mod)

feature_consumer_main = sys.modules["feature_consumer_main"]
FeatureAggregator = feature_consumer_main.FeatureAggregator
BATCH_SIZE = feature_consumer_main.BATCH_SIZE
BATCH_TIMEOUT_MS = feature_consumer_main.BATCH_TIMEOUT_MS


@pytest.fixture
def redis_client():
    return fakeredis.FakeRedis(decode_responses=True)


@pytest.fixture
def aggregator(redis_client):
    return FeatureAggregator(redis_client)


RIDE_EVENT = {
    "h3_res8": "881e2040c9fffff",
    "timestamp": "2024-06-15T10:15:00+00:00",
    "zone": "Sol",
}

DRIVER_EVENT_IDLE = {
    "driver_id": "d_00001",
    "h3_res8": "881e2040c9fffff",
    "timestamp": "2024-06-15T10:15:00+00:00",
    "status": "available",
    "idle_seconds": 60,
}

DRIVER_EVENT_ON_TRIP = {
    "driver_id": "d_00002",
    "h3_res8": "881e2040c9fffff",
    "timestamp": "2024-06-15T10:15:00+00:00",
    "status": "on_trip",
    "idle_seconds": 0,
}


class TestRideEventAggregation:
    def test_ride_event_increments_ride_requests(self, aggregator):
        result = aggregator.add_ride_event(RIDE_EVENT)
        assert result is True
        key = next(iter(aggregator.pending_updates))
        assert aggregator.pending_updates[key]["ride_requests"] == 1

    def test_ride_event_missing_h3_returns_false(self, aggregator):
        bad = {"timestamp": "2024-06-15T10:15:00+00:00"}
        assert aggregator.add_ride_event(bad) is False

    def test_ride_event_missing_timestamp_returns_false(self, aggregator):
        bad = {"h3_res8": "881e2040c9fffff"}
        assert aggregator.add_ride_event(bad) is False

    def test_multiple_ride_events_accumulate(self, aggregator):
        aggregator.add_ride_event(RIDE_EVENT)
        aggregator.add_ride_event(RIDE_EVENT)
        key = next(iter(aggregator.pending_updates))
        assert aggregator.pending_updates[key]["ride_requests"] == 2


class TestDriverEventAggregation:
    def test_driver_idle_increments_idle_drivers(self, aggregator):
        aggregator.add_driver_event(DRIVER_EVENT_IDLE)
        key = next(iter(aggregator.pending_updates))
        assert aggregator.pending_updates[key]["idle_drivers"] == 1

    def test_driver_on_trip_increments_on_trip_drivers(self, aggregator):
        aggregator.add_driver_event(DRIVER_EVENT_ON_TRIP)
        key = next(iter(aggregator.pending_updates))
        assert aggregator.pending_updates[key]["on_trip_drivers"] == 1

    def test_driver_dedup_same_window(self, aggregator):
        """Same driver in same window should only be counted once."""
        aggregator.add_driver_event(DRIVER_EVENT_IDLE)
        aggregator.add_driver_event(DRIVER_EVENT_IDLE)  # duplicate
        key = next(iter(aggregator.pending_updates))
        assert aggregator.pending_updates[key]["idle_drivers"] == 1

    def test_driver_dedup_different_drivers(self, aggregator):
        """Different drivers in same window should each be counted."""
        d1 = dict(DRIVER_EVENT_IDLE, driver_id="d_00001")
        d2 = dict(DRIVER_EVENT_IDLE, driver_id="d_00002")
        aggregator.add_driver_event(d1)
        aggregator.add_driver_event(d2)
        key = next(iter(aggregator.pending_updates))
        assert aggregator.pending_updates[key]["idle_drivers"] == 2


class TestFlushBehavior:
    def test_flush_triggered_by_batch_size(self, aggregator):
        for _ in range(BATCH_SIZE):
            aggregator.add_ride_event(RIDE_EVENT)
        assert aggregator.should_flush()

    def test_flush_triggered_by_timeout(self, aggregator):
        aggregator.last_flush = time.time() - (BATCH_TIMEOUT_MS / 1000) - 0.1
        assert aggregator.should_flush()

    def test_flush_writes_to_redis(self, aggregator, redis_client):
        aggregator.add_ride_event(RIDE_EVENT)
        count = aggregator.flush()
        assert count > 0
        # Check that Redis has at least one key
        keys = redis_client.keys("*madrid*")
        assert len(keys) > 0

    def test_flush_sets_ttl(self, aggregator, redis_client):
        aggregator.add_ride_event(RIDE_EVENT)
        aggregator.flush()
        keys = redis_client.keys("*madrid*")
        for key in keys:
            assert redis_client.ttl(key) > 0

    def test_pending_cleared_after_flush(self, aggregator):
        aggregator.add_ride_event(RIDE_EVENT)
        aggregator.flush()
        assert len(aggregator.pending_updates) == 0
        assert aggregator.events_since_flush == 0

    def test_flush_empty_returns_zero(self, aggregator):
        assert aggregator.flush() == 0
