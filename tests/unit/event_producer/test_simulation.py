"""Tests for DriverSimulator and spatial helpers in services/event-producer/main.py"""
import sys
import os
import importlib.util
import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

_EP_PATH = os.path.join(ROOT, "services", "event-producer", "main.py")
if "event_producer_main" not in sys.modules:
    _spec = importlib.util.spec_from_file_location("event_producer_main", _EP_PATH)
    _mod = importlib.util.module_from_spec(_spec)
    sys.modules["event_producer_main"] = _mod
    _spec.loader.exec_module(_mod)

ep = sys.modules["event_producer_main"]
DriverSimulator = ep.DriverSimulator
sample_urban_background = ep.sample_urban_background
normalize_weights = ep.normalize_weights
MADRID_ZONES = ep.MADRID_ZONES
MADRID_BOUNDS = ep.MADRID_BOUNDS


class TestSampleUrbanBackground:
    def test_exits_without_infinite_loop(self):
        """Must return a result and not hang."""
        lat, lng = sample_urban_background()
        assert isinstance(lat, float)
        assert isinstance(lng, float)

    def test_result_within_madrid_bounds(self):
        lat, lng = sample_urban_background()
        assert MADRID_BOUNDS["lat_min"] <= lat <= MADRID_BOUNDS["lat_max"]
        assert MADRID_BOUNDS["lng_min"] <= lng <= MADRID_BOUNDS["lng_max"]

    def test_runs_multiple_times_consistently(self):
        for _ in range(10):
            lat, lng = sample_urban_background()
            assert MADRID_BOUNDS["lat_min"] <= lat <= MADRID_BOUNDS["lat_max"]


class TestNormalizeWeights:
    def test_weights_sum_to_one(self):
        weights = normalize_weights(MADRID_ZONES, "demand_weight")
        assert abs(sum(weights) - 1.0) < 1e-9

    def test_all_weights_positive(self):
        weights = normalize_weights(MADRID_ZONES, "driver_weight")
        assert all(w > 0 for w in weights)

    def test_proportional_ordering(self):
        weights = normalize_weights(MADRID_ZONES, "demand_weight")
        raw = [z.demand_weight for z in MADRID_ZONES]
        # Highest raw weight → highest normalized weight
        max_raw_idx = raw.index(max(raw))
        assert weights[max_raw_idx] == max(weights)


class TestDriverSimulator:
    @pytest.fixture
    def sim(self):
        return DriverSimulator(num_drivers=20, ping_interval=5, event_interval=2)

    def test_init_creates_correct_number_of_drivers(self, sim):
        assert len(sim.drivers) == 20

    def test_driver_location_in_madrid_bounds_after_100_ticks(self, sim):
        for _ in range(100):
            sim.generate_events(12, 1.0)
        for driver in sim.drivers.values():
            # Generous bounds to allow for drift
            assert 39.0 < driver["lat"] < 42.0
            assert -5.5 < driver["lng"] < -2.0

    def test_driver_status_transitions_over_time(self, sim):
        """After many ticks, some drivers should have been on_trip at least once."""
        seen_on_trip = False
        for _ in range(200):
            events = sim.generate_events(12, 1.5)
            if any(e["status"] == "on_trip" for e in events):
                seen_on_trip = True
                break
        assert seen_on_trip

    def test_generate_events_returns_list(self, sim):
        events = sim.generate_events(12, 1.0)
        assert isinstance(events, list)

    def test_events_have_required_fields(self, sim):
        for _ in range(3):
            events = sim.generate_events(12, 1.0)
            for event in events:
                assert "driver_id" in event
                assert "lat" in event
                assert "lng" in event
                assert "status" in event
                assert "h3_res8" in event

    def test_get_stats_returns_consistent_counts(self, sim):
        stats = sim.get_stats()
        assert stats["total"] == 20
        assert stats["online"] <= 20
        assert stats["available"] + stats["on_trip"] == stats["online"]
