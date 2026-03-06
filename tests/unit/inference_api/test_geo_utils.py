"""Tests for services/inference-api/geo/utils.py"""
import math
from geo.utils import haversine_km


class TestHaversineKm:
    def test_same_point_is_zero(self):
        assert haversine_km(40.4169, -3.7034, 40.4169, -3.7034) == 0.0

    def test_known_distance_madrid_center_to_barajas(self):
        """Madrid Puerta del Sol to Barajas T4 is ~14 km by straight line."""
        dist = haversine_km(40.4168, -3.7038, 40.4929, -3.5922)
        assert 11.0 < dist < 17.0

    def test_symmetry(self):
        """Distance A→B equals B→A."""
        d1 = haversine_km(40.4169, -3.7034, 40.4722, -3.6824)
        d2 = haversine_km(40.4722, -3.6824, 40.4169, -3.7034)
        assert abs(d1 - d2) < 1e-9

    def test_returns_positive(self):
        dist = haversine_km(40.0, -3.0, 41.0, -4.0)
        assert dist > 0.0

    def test_antipodal_no_domain_error(self):
        """Antipodal points should not raise ValueError from asin(sqrt(>1))."""
        # Points nearly antipodal — a ≈ 1.0
        dist = haversine_km(90.0, 0.0, -90.0, 0.0)
        assert abs(dist - math.pi * 6371.0) < 1.0  # ≈ half Earth circumference

    def test_short_distance_is_accurate(self):
        """1 degree latitude ≈ 111 km."""
        dist = haversine_km(40.0, 0.0, 41.0, 0.0)
        assert 109.0 < dist < 113.0
