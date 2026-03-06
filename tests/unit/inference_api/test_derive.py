"""Tests for services/inference-api/derive.py"""
from derive import derive_features, safe_div


class TestSafeDiv:
    def test_normal_division(self):
        assert safe_div(10.0, 2.0) == 5.0

    def test_zero_denominator_returns_zero(self):
        assert safe_div(10.0, 0.0) == 0.0

    def test_zero_numerator(self):
        assert safe_div(0.0, 5.0) == 0.0

    def test_both_zero(self):
        assert safe_div(0.0, 0.0) == 0.0


class TestDeriveFeatures:
    def test_normal_supply_demand_ratio(self):
        raw = {
            "ride_requests": "10",
            "idle_drivers": "30",
            "on_trip_drivers": "5",
            "deadhead_km_sum": "30.0",
            "idle_events": "10",
        }
        result = derive_features(raw)
        assert result["supply_demand_ratio"] == 3.0
        assert result["surge_pressure"] == 0.0

    def test_empty_redis_returns_zero_defaults(self):
        result = derive_features({})
        assert result["ride_requests"] == 0
        assert result["idle_drivers"] == 0
        assert result["supply_demand_ratio"] == 0.0
        assert result["surge_pressure"] == 1.0  # zero supply → full pressure
        assert result["deadhead_km_avg"] == 0.0

    def test_surge_pressure_clamped_min(self):
        """Excess supply (ratio >> 3) → surge_pressure = 0.0"""
        raw = {"idle_drivers": "100", "ride_requests": "1"}
        result = derive_features(raw)
        assert result["surge_pressure"] == 0.0

    def test_surge_pressure_clamped_max(self):
        """Zero idle drivers → surge_pressure = 1.0"""
        raw = {"idle_drivers": "0", "ride_requests": "10"}
        result = derive_features(raw)
        assert result["surge_pressure"] == 1.0

    def test_surge_pressure_midpoint(self):
        """sd_ratio = 1.5 (half of target 3) → pressure = 0.5"""
        raw = {"idle_drivers": "15", "ride_requests": "10"}
        result = derive_features(raw)
        assert abs(result["surge_pressure"] - 0.5) < 0.01

    def test_deadhead_avg_calculation(self):
        raw = {
            "deadhead_km_sum": "30.0",
            "idle_events": "10",
        }
        result = derive_features(raw)
        assert result["deadhead_km_avg"] == 3.0

    def test_deadhead_zero_idle_events_returns_zero(self):
        raw = {"deadhead_km_sum": "30.0", "idle_events": "0"}
        result = derive_features(raw)
        assert result["deadhead_km_avg"] == 0.0

    def test_on_trip_drivers_included(self):
        raw = {"on_trip_drivers": "7"}
        result = derive_features(raw)
        assert result["on_trip_drivers"] == 7
