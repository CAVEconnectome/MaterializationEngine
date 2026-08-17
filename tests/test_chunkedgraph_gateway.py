"""The gateway must read from the chunkedgraph READ deployment by default.

Every call this gateway makes is a read: is_latest_roots, get_roots, get_past_ids,
get_root_timestamps, get_delta_roots. It previously defaulted to http://pychunkedgraph-service/,
the write deployment, which autoscales from a single replica and has no preStop hook. Observed on
minniev7 (2026-08-17): while it scaled, materialize's internal call was refused --

    HTTPConnectionPool(host='pychunkedgraph-service', port=80): Max retries exceeded with url:
    /segmentation/api/v1/table/minnie3_v1/is_latest_roots ... [Errno 111] Connection refused

-- and materialize returned that to its callers as a 500, which the skeleton cache then dropped.
"""

import importlib

import pytest

from materializationengine import chunkedgraph_gateway as gw


@pytest.fixture
def env(monkeypatch):
    def _set(**pairs):
        for k, v in pairs.items():
            if v is None:
                monkeypatch.delenv(k, raising=False)
            else:
                monkeypatch.setenv(k, v)

    _set(PCG_SERVER_URL=None, LOCAL_SERVER_URL=None)
    return _set


class TestServiceResolution:
    def test_defaults_to_the_read_deployment(self, env):
        assert gw._resolve_pcg_service() == "http://pychunkedgraph-read-service/"

    def test_default_is_not_the_write_deployment(self, env):
        """The specific regression: the write service scales from 1 and drops connections."""
        assert "pychunkedgraph-service" not in gw._resolve_pcg_service()

    def test_pcg_server_url_overrides(self, env):
        env(PCG_SERVER_URL="http://somewhere-else/")

        assert gw._resolve_pcg_service() == "http://somewhere-else/"

    def test_local_server_url_cannot_route_the_chunkedgraph_out_of_the_cluster(self, env):
        """LOCAL_SERVER_URL names the PUBLIC local server (config.cfg sets it to https://<domain>).

        Resolving the chunkedgraph through it would leave the cluster and re-enter via the
        ingress rather than going straight to the service over cluster DNS. It must be ignored.
        """
        env(LOCAL_SERVER_URL="https://minnie.microns-daf.com")

        assert gw._resolve_pcg_service() == "http://pychunkedgraph-read-service/"

    def test_pcg_server_url_is_the_only_override(self, env):
        env(LOCAL_SERVER_URL="https://minnie.microns-daf.com", PCG_SERVER_URL="http://pcg-read/")

        assert gw._resolve_pcg_service() == "http://pcg-read/"

    def test_the_default_is_a_cluster_local_address(self, env):
        """No scheme-host that would exit the cluster: plain http to a k8s service name."""
        url = gw._resolve_pcg_service()

        assert url.startswith("http://"), url
        assert "." not in url.split("//", 1)[1].rstrip("/"), f"{url} looks like an external domain"

    def test_module_level_constant_uses_the_resolver(self, env):
        reloaded = importlib.reload(gw)
        try:
            assert reloaded.PCG_SERVICE == "http://pychunkedgraph-read-service/"
        finally:
            importlib.reload(gw)  # restore for any later test in the session

    def test_the_gateway_passes_the_address_to_the_client(self, env, monkeypatch):
        captured = {}

        class FakeClient:
            def __init__(self, server_address, table_name=None, auth_client=None):
                captured["server_address"] = server_address

        monkeypatch.setattr(gw, "ChunkedGraphClient", FakeClient)
        gateway = gw.ChunkedGraphGateway.__new__(gw.ChunkedGraphGateway)
        gateway._cg = {}
        gateway.server_address = gw._resolve_pcg_service()
        gateway.auth = None

        gateway.init_pcg("minnie3_v1")

        assert captured["server_address"] == "http://pychunkedgraph-read-service/"
