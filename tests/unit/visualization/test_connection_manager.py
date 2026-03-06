"""Tests for ConnectionManager in services/visualization/main.py"""
import sys
import os
import importlib.util
import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

_VIZ_PATH = os.path.join(ROOT, "services", "visualization", "main.py")
if "visualization_main" not in sys.modules:
    _spec = importlib.util.spec_from_file_location("visualization_main", _VIZ_PATH)
    _mod = importlib.util.module_from_spec(_spec)
    sys.modules["visualization_main"] = _mod
    _spec.loader.exec_module(_mod)

viz = sys.modules["visualization_main"]
ConnectionManager = viz.ConnectionManager


def _make_ws():
    """Create a mock WebSocket."""
    ws = MagicMock()
    ws.accept = AsyncMock()
    ws.send_text = AsyncMock()
    ws.send_json = AsyncMock()
    return ws


class TestConnectionManagerConnect:
    async def test_connect_adds_to_active_connections(self):
        manager = ConnectionManager()
        ws = _make_ws()
        await manager.connect(ws)
        assert ws in manager.active_connections

    async def test_connect_sets_default_window(self):
        manager = ConnectionManager()
        ws = _make_ws()
        await manager.connect(ws)
        assert manager.client_windows[ws] == 5

    async def test_connect_multiple_clients(self):
        manager = ConnectionManager()
        ws1, ws2 = _make_ws(), _make_ws()
        await manager.connect(ws1)
        await manager.connect(ws2)
        assert len(manager.active_connections) == 2


class TestConnectionManagerDisconnect:
    async def test_disconnect_removes_connection(self):
        manager = ConnectionManager()
        ws = _make_ws()
        await manager.connect(ws)
        manager.disconnect(ws)
        assert ws not in manager.active_connections

    async def test_disconnect_removes_window_preference(self):
        manager = ConnectionManager()
        ws = _make_ws()
        await manager.connect(ws)
        manager.disconnect(ws)
        assert ws not in manager.client_windows

    def test_disconnect_nonexistent_is_safe(self):
        manager = ConnectionManager()
        ws = _make_ws()
        manager.disconnect(ws)  # should not raise

    async def test_disconnect_updates_connection_count(self):
        manager = ConnectionManager()
        ws1, ws2 = _make_ws(), _make_ws()
        await manager.connect(ws1)
        await manager.connect(ws2)
        manager.disconnect(ws1)
        assert len(manager.active_connections) == 1


class TestConnectionManagerBroadcast:
    async def test_broadcast_sends_to_all_connections(self):
        manager = ConnectionManager()
        ws1, ws2 = _make_ws(), _make_ws()
        await manager.connect(ws1)
        await manager.connect(ws2)

        cached = {5: {"type": "demand_update", "data": []}}
        await manager.broadcast(cached)

        ws1.send_text.assert_called_once()
        ws2.send_text.assert_called_once()

    async def test_broadcast_removes_dead_connections(self):
        manager = ConnectionManager()
        ws = _make_ws()
        await manager.connect(ws)
        ws.send_text.side_effect = Exception("Connection closed")

        cached = {5: {"type": "demand_update"}}
        await manager.broadcast(cached)

        assert ws not in manager.active_connections

    async def test_broadcast_skips_missing_window_data(self):
        manager = ConnectionManager()
        ws = _make_ws()
        await manager.connect(ws)
        manager.set_window(ws, 15)

        # cached only has window 5, not 15
        cached = {5: {"type": "demand_update"}}
        await manager.broadcast(cached)
        ws.send_text.assert_not_called()

    async def test_broadcast_copies_list_before_iterating(self):
        """Disconnecting during broadcast should not cause RuntimeError."""
        manager = ConnectionManager()
        ws = _make_ws()
        await manager.connect(ws)

        original_disconnect = manager.disconnect
        calls = []

        def disconnect_and_track(conn):
            calls.append(conn)
            original_disconnect(conn)

        ws.send_text.side_effect = Exception("dead")
        manager.disconnect = disconnect_and_track

        cached = {5: {"type": "demand_update"}}
        # Should not raise "list changed size during iteration"
        await manager.broadcast(cached)
        assert ws in calls

    async def test_broadcast_empty_connections_is_noop(self):
        manager = ConnectionManager()
        # Should not raise when no connections
        await manager.broadcast({5: {"type": "demand_update"}})


class TestConnectionManagerSetWindow:
    async def test_set_window_updates_preference(self):
        manager = ConnectionManager()
        ws = _make_ws()
        await manager.connect(ws)
        manager.set_window(ws, 15)
        assert manager.client_windows[ws] == 15

    def test_set_window_on_unknown_client_is_noop(self):
        manager = ConnectionManager()
        ws = _make_ws()
        manager.set_window(ws, 15)  # should not raise or add entry
        assert ws not in manager.client_windows
