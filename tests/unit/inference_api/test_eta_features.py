"""Tests for services/inference-api/eta/features.py"""
from datetime import datetime, timezone
from eta.features import assemble_pickup_features, assemble_dropoff_features


FEATURES_5M = {
    "supply_demand_ratio": 2.0,
    "surge_pressure": 0.33,
    "deadhead_km_avg": 1.5,
    "idle_drivers": 20,
    "ride_requests": 10,
    "on_trip_drivers": 5,
}


class TestAssemblePickupFeatures:
    def test_returns_all_required_keys(self):
        result = assemble_pickup_features(5.0, FEATURES_5M)
        for key in ["trip_distance_km", "supply_demand_ratio", "surge_pressure",
                    "deadhead_km_avg", "idle_drivers", "ride_requests"]:
            assert key in result

    def test_trip_distance_passed_through(self):
        result = assemble_pickup_features(7.5, FEATURES_5M)
        assert result["trip_distance_km"] == 7.5

    def test_features_5m_values_copied(self):
        result = assemble_pickup_features(5.0, FEATURES_5M)
        assert result["supply_demand_ratio"] == FEATURES_5M["supply_demand_ratio"]
        assert result["surge_pressure"] == FEATURES_5M["surge_pressure"]
        assert result["idle_drivers"] == FEATURES_5M["idle_drivers"]


class TestAssembleDropoffFeatures:
    def test_returns_all_required_keys(self):
        ts = datetime(2024, 6, 15, 14, 0, 0, tzinfo=timezone.utc)
        result = assemble_dropoff_features(5.0, FEATURES_5M, ts)
        for key in ["trip_distance_km", "surge_pressure", "hour_of_day", "is_weekend"]:
            assert key in result

    def test_hour_of_day_extracted_from_ts(self):
        ts = datetime(2024, 6, 15, 18, 0, 0, tzinfo=timezone.utc)
        result = assemble_dropoff_features(5.0, FEATURES_5M, ts)
        assert result["hour_of_day"] == 18

    def test_weekday_is_zero(self):
        ts = datetime(2024, 6, 17, 10, 0, 0, tzinfo=timezone.utc)  # Monday
        result = assemble_dropoff_features(5.0, FEATURES_5M, ts)
        assert result["is_weekend"] == 0

    def test_weekend_is_one(self):
        ts = datetime(2024, 6, 15, 10, 0, 0, tzinfo=timezone.utc)  # Saturday
        result = assemble_dropoff_features(5.0, FEATURES_5M, ts)
        assert result["is_weekend"] == 1
