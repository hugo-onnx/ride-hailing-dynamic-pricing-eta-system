"""Tests for services/inference-api/features.py"""
import pytest
from datetime import datetime, timezone, timedelta

from features import fetch_window, floor_completed_window, redis_key


def _make_key(fake_redis, h3_index: str, window: int, ts: datetime, data: dict):
    """Helper to write feature data for a specific window into fake Redis."""
    window_start = floor_completed_window(ts, window)
    key = redis_key("madrid", h3_index, window, window_start)
    fake_redis.hset(key, mapping=data)
    return window_start


class TestFetchWindow:
    H3 = "881e2040c9fffff"
    DATA = {
        "ride_requests": "5",
        "idle_drivers": "10",
        "on_trip_drivers": "2",
    }

    def test_fetch_window_returns_current_window(self, fake_redis):
        ts = datetime.now(timezone.utc)
        _make_key(fake_redis, self.H3, 5, ts, self.DATA)

        result = fetch_window(fake_redis, "madrid", self.H3, 5, ts)
        assert result["ride_requests"] == "5"
        assert result["idle_drivers"] == "10"

    def test_fetch_window_falls_back_to_previous_when_empty(self, fake_redis):
        ts = datetime.now(timezone.utc)
        # Write data for the window BEFORE the current completed window
        window_start = floor_completed_window(ts, 5)
        prev_start = window_start - timedelta(minutes=5)
        prev_key = redis_key("madrid", self.H3, 5, prev_start)
        fake_redis.hset(prev_key, mapping=self.DATA)

        result = fetch_window(fake_redis, "madrid", self.H3, 5, ts)
        assert result["ride_requests"] == "5"

    def test_fetch_window_returns_empty_if_no_data(self, fake_redis):
        ts = datetime.now(timezone.utc)
        result = fetch_window(fake_redis, "madrid", self.H3, 5, ts)
        assert result == {}

    def test_redis_key_format(self):
        ts = datetime(2024, 6, 15, 10, 15, 0, tzinfo=timezone.utc)
        key = redis_key("madrid", self.H3, 5, ts)
        assert key.startswith("madrid:")
        assert self.H3 in key
        assert "5m:" in key

    def test_fetch_window_records_freshness_on_hit(self, fake_redis):
        """Freshness metric key should be created in Redis after a successful fetch."""
        ts = datetime.now(timezone.utc)
        _make_key(fake_redis, self.H3, 5, ts, self.DATA)

        fetch_window(fake_redis, "madrid", self.H3, 5, ts)
        # record_feature_freshness writes to metrics:feature_freshness:{window}m
        assert fake_redis.exists("metrics:feature_freshness:5m")

    def test_different_windows_use_different_keys(self, fake_redis):
        ts = datetime.now(timezone.utc)
        for window in [1, 5, 15]:
            _make_key(fake_redis, self.H3, window, ts, {"ride_requests": str(window)})

        r1 = fetch_window(fake_redis, "madrid", self.H3, 1, ts)
        r5 = fetch_window(fake_redis, "madrid", self.H3, 5, ts)
        r15 = fetch_window(fake_redis, "madrid", self.H3, 15, ts)
        assert r1["ride_requests"] == "1"
        assert r5["ride_requests"] == "5"
        assert r15["ride_requests"] == "15"
