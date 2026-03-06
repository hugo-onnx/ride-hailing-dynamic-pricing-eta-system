"""Tests for services/inference-api/routing/osrm.py"""
import pytest
import requests
from unittest.mock import MagicMock, patch

from routing.osrm import OSRMClient, OSRMError, RouteResult, get_osrm_client


def _mock_response(status_code=200, json_data=None):
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data or {}
    if status_code != 200:
        resp.raise_for_status.side_effect = requests.exceptions.HTTPError(
            f"HTTP {status_code}"
        )
    else:
        resp.raise_for_status.return_value = None
    return resp


OSRM_SUCCESS = {
    "code": "Ok",
    "routes": [{"distance": 12000.0, "duration": 720.0}],
}


class TestOSRMClientGetRoute:
    def setup_method(self):
        self.client = OSRMClient(host="http://fake-osrm:5000", timeout=2.0)

    def test_route_success_returns_result(self):
        with patch.object(self.client._session, "get", return_value=_mock_response(200, OSRM_SUCCESS)):
            result = self.client.get_route((-3.7038, 40.4168), (-3.5922, 40.4929))
        assert isinstance(result, RouteResult)
        assert result.distance_km == 12.0
        assert result.duration_min == 12.0
        assert result.duration_s == 720.0

    def test_route_non200_raises_osrm_error(self):
        with patch.object(self.client._session, "get", return_value=_mock_response(500)):
            with pytest.raises(OSRMError):
                self.client.get_route((-3.7038, 40.4168), (-3.5922, 40.4929))

    def test_route_timeout_raises_osrm_error(self):
        with patch.object(self.client._session, "get",
                          side_effect=requests.exceptions.Timeout()):
            with pytest.raises(OSRMError, match="timed out"):
                self.client.get_route((-3.7038, 40.4168), (-3.5922, 40.4929))

    def test_route_connection_error_raises_osrm_error(self):
        with patch.object(self.client._session, "get",
                          side_effect=requests.exceptions.ConnectionError("refused")):
            with pytest.raises(OSRMError, match="connection failed"):
                self.client.get_route((-3.7038, 40.4168), (-3.5922, 40.4929))

    def test_route_osrm_code_not_ok_raises_osrm_error(self):
        bad_response = {"code": "NoRoute", "message": "No route found"}
        with patch.object(self.client._session, "get",
                          return_value=_mock_response(200, bad_response)):
            with pytest.raises(OSRMError, match="No route found"):
                self.client.get_route((-3.7038, 40.4168), (-3.5922, 40.4929))

    def test_route_empty_routes_list_raises_osrm_error(self):
        no_routes = {"code": "Ok", "routes": []}
        with patch.object(self.client._session, "get",
                          return_value=_mock_response(200, no_routes)):
            with pytest.raises(OSRMError, match="No route"):
                self.client.get_route((-3.7038, 40.4168), (-3.5922, 40.4929))


class TestOSRMClientHealthCheck:
    def setup_method(self):
        self.client = OSRMClient(host="http://fake-osrm:5000", timeout=2.0)

    def test_health_check_success(self):
        with patch.object(self.client._session, "get",
                          return_value=_mock_response(200)):
            assert self.client.health_check() is True

    def test_health_check_non200_returns_false(self):
        with patch.object(self.client._session, "get",
                          return_value=_mock_response(500)):
            assert self.client.health_check() is False

    def test_health_check_exception_returns_false(self):
        with patch.object(self.client._session, "get",
                          side_effect=Exception("Connection refused")):
            assert self.client.health_check() is False


class TestGetOSRMClientSingleton:
    def test_singleton_returns_same_instance(self):
        import routing.osrm as osrm_module
        osrm_module._client = None  # reset
        c1 = get_osrm_client()
        c2 = get_osrm_client()
        assert c1 is c2
        osrm_module._client = None  # cleanup
