"""Tests for services/inference-api/pricing.py"""
from pricing import compute_price_multiplier


class TestComputePriceMultiplier:
    def _features(self, sd_ratio=3.0, deadhead=0.0, requests=10):
        return {
            "supply_demand_ratio": sd_ratio,
            "deadhead_km_avg": deadhead,
            "ride_requests": requests,
            "surge_pressure": max(0.0, min(1.0, (3.0 - sd_ratio) / 3.0)),
            "idle_drivers": 0,
            "on_trip_drivers": 0,
        }

    def test_normal_market_no_surge(self):
        result = compute_price_multiplier(self._features(sd_ratio=3.0))
        assert result["multiplier"] == 1.0
        assert result["surge_level"] == "normal"

    def test_shortage_increases_multiplier(self):
        result = compute_price_multiplier(self._features(sd_ratio=0.5))
        assert result["multiplier"] > 1.0

    def test_max_surge_capped_at_2(self):
        """Extreme shortage + high deadhead together should hit 2.0 ceiling."""
        result = compute_price_multiplier(self._features(sd_ratio=0.0, deadhead=10.0, requests=20))
        assert result["multiplier"] == 2.0

    def test_min_surge_floor_at_1(self):
        """Oversupply should not go below 1.0."""
        result = compute_price_multiplier(self._features(sd_ratio=10.0))
        assert result["multiplier"] == 1.0

    def test_deadhead_adds_to_multiplier(self):
        no_deadhead = compute_price_multiplier(self._features(deadhead=0.0))
        with_deadhead = compute_price_multiplier(self._features(deadhead=3.0))
        assert with_deadhead["multiplier"] > no_deadhead["multiplier"]

    def test_low_volume_dampening(self):
        """< 5 requests halves the surge delta."""
        high_vol = compute_price_multiplier(self._features(sd_ratio=0.5, requests=10))
        low_vol = compute_price_multiplier(self._features(sd_ratio=0.5, requests=3))
        assert low_vol["multiplier"] < high_vol["multiplier"]
        assert "low_volume_dampening" in low_vol["reasons"]

    def test_surge_level_high(self):
        result = compute_price_multiplier(self._features(sd_ratio=0.0, requests=20))
        assert result["surge_level"] == "high"

    def test_surge_level_moderate(self):
        result = compute_price_multiplier(self._features(sd_ratio=0.3, requests=20))
        assert result["surge_level"] in ("moderate", "high")

    def test_surge_level_normal(self):
        result = compute_price_multiplier(self._features(sd_ratio=3.0))
        assert result["surge_level"] == "normal"

    def test_reasons_list_populated_on_shortage(self):
        result = compute_price_multiplier(self._features(sd_ratio=0.5, requests=20))
        assert any("supply_demand_shortage" in r for r in result["reasons"])

    def test_reasons_empty_on_normal_market(self):
        result = compute_price_multiplier(self._features(sd_ratio=3.0, requests=10))
        assert result["reasons"] == []
