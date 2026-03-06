"""Tests for services/inference-api/eta/dropoff_adjust.py"""
from eta.dropoff_adjust import adjust_dropoff_eta, compute_congestion_factor, GAMMA


class TestAdjustDropoffEta:
    def test_no_pressure_no_adjustment(self):
        result = adjust_dropoff_eta(600.0, 0.0)
        assert result == 600.0

    def test_full_pressure_applies_gamma(self):
        result = adjust_dropoff_eta(600.0, 1.0)
        assert result == 600.0 * (1.0 + GAMMA)

    def test_half_pressure_partial_adjustment(self):
        result = adjust_dropoff_eta(600.0, 0.5)
        assert abs(result - 600.0 * (1.0 + GAMMA * 0.5)) < 0.01

    def test_result_always_gte_input(self):
        """Adjusted ETA should never be shorter than free-flow."""
        for pressure in [0.0, 0.25, 0.5, 0.75, 1.0]:
            assert adjust_dropoff_eta(300.0, pressure) >= 300.0

    def test_custom_gamma(self):
        result = adjust_dropoff_eta(600.0, 1.0, gamma=0.3)
        assert abs(result - 600.0 * 1.3) < 0.01


class TestComputeCongestionFactor:
    def test_no_pressure_factor_is_one(self):
        assert compute_congestion_factor(0.0) == 1.0

    def test_full_pressure_factor_is_one_plus_gamma(self):
        assert abs(compute_congestion_factor(1.0) - (1.0 + GAMMA)) < 1e-9

    def test_factor_increases_with_pressure(self):
        f1 = compute_congestion_factor(0.3)
        f2 = compute_congestion_factor(0.7)
        assert f2 > f1

    def test_consistent_with_adjust_dropoff_eta(self):
        """factor * duration should equal adjust_dropoff_eta result."""
        pressure = 0.6
        factor = compute_congestion_factor(pressure)
        adjusted = adjust_dropoff_eta(500.0, pressure)
        assert abs(adjusted - 500.0 * factor) < 0.01
