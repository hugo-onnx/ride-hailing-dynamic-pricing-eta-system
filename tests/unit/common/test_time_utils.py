"""Tests for services/common/time_utils.py"""
import pytest
from datetime import datetime, timezone

from services.common.time_utils import floor_timestamp, window_ttl_seconds


class TestFloorTimestamp:
    def test_floor_1m_window(self):
        ts = datetime(2024, 6, 15, 14, 32, 45, tzinfo=timezone.utc)
        result = floor_timestamp(ts, 1)
        assert result == datetime(2024, 6, 15, 14, 32, 0, tzinfo=timezone.utc)

    def test_floor_5m_window(self):
        ts = datetime(2024, 6, 15, 14, 32, 45, tzinfo=timezone.utc)
        result = floor_timestamp(ts, 5)
        assert result == datetime(2024, 6, 15, 14, 30, 0, tzinfo=timezone.utc)

    def test_floor_15m_window(self):
        ts = datetime(2024, 6, 15, 14, 32, 45, tzinfo=timezone.utc)
        result = floor_timestamp(ts, 15)
        assert result == datetime(2024, 6, 15, 14, 30, 0, tzinfo=timezone.utc)

    def test_floor_15m_window_second_slot(self):
        ts = datetime(2024, 6, 15, 14, 47, 0, tzinfo=timezone.utc)
        result = floor_timestamp(ts, 15)
        assert result == datetime(2024, 6, 15, 14, 45, 0, tzinfo=timezone.utc)

    def test_floor_naive_datetime_gets_utc(self):
        ts = datetime(2024, 6, 15, 14, 32, 45)  # naive
        result = floor_timestamp(ts, 5)
        assert result.tzinfo == timezone.utc

    def test_floor_zeros_seconds_and_microseconds(self):
        ts = datetime(2024, 6, 15, 14, 32, 45, 999999, tzinfo=timezone.utc)
        result = floor_timestamp(ts, 5)
        assert result.second == 0
        assert result.microsecond == 0

    def test_floor_zero_window_raises_value_error(self):
        ts = datetime(2024, 6, 15, 14, 32, 45, tzinfo=timezone.utc)
        with pytest.raises(ValueError, match="window_minutes must be positive"):
            floor_timestamp(ts, 0)

    def test_floor_negative_window_raises_value_error(self):
        ts = datetime(2024, 6, 15, 14, 32, 45, tzinfo=timezone.utc)
        with pytest.raises(ValueError, match="window_minutes must be positive"):
            floor_timestamp(ts, -5)


class TestWindowTtlSeconds:
    def test_window_ttl_1m(self):
        assert window_ttl_seconds(1) == 120

    def test_window_ttl_5m(self):
        assert window_ttl_seconds(5) == 600

    def test_window_ttl_15m(self):
        assert window_ttl_seconds(15) == 1800

    def test_window_ttl_exceeds_window_duration(self):
        """TTL should be larger than the window itself to allow overlap."""
        for w in [1, 5, 15]:
            assert window_ttl_seconds(w) > w * 60
