"""Registering exported lakes with the catalog service, against an in-memory stand-in for
caveclient's CatalogClient."""

import uuid
from unittest import mock

import pytest

from materializationengine import catalog_service as cs
from materializationengine import deltalake_catalog as cat


class FakeCatalog:
    """The CatalogClient methods catalog_service uses, over a dict of assets."""

    def __init__(self):
        self.assets = {}
        self.calls = []

    def check_uri(self, uri):
        for a in self.assets.values():
            if a["uri"] == uri:
                return {"available": False, "reason": "duplicate_uri", "existing_id": a["id"]}
        return {"available": True}

    def get_asset(self, asset_id):
        return self.assets[asset_id]

    def delete_asset(self, asset_id):
        self.calls.append(("delete", asset_id))
        del self.assets[asset_id]

    def register_table(self, name, uri, format, is_managed, mat_version=None, source="user",
                       properties=None, **kwargs):
        if self.check_uri(uri).get("existing_id"):
            raise RuntimeError("409: duplicate uri")
        asset = {"id": uuid.uuid4().hex, "name": name, "uri": uri, "format": format,
                 "is_managed": is_managed, "mat_version": mat_version, "source": source,
                 "properties": properties or {}}
        self.calls.append(("register", name))
        self.assets[asset["id"]] = asset
        return asset

    def list_tables(self, mat_version=None, source=None, **kwargs):
        return [a for a in self.assets.values()
                if (mat_version is None or a["mat_version"] == mat_version)
                and (source is None or a["source"] == source)]


@pytest.fixture
def fake(monkeypatch):
    fake = FakeCatalog()
    monkeypatch.setattr(cs, "_client", lambda datastack: fake)
    monkeypatch.setattr(cs, "catalog_url", lambda: "https://local.cave.com/catalog")
    return fake


def row(table="synapses", spec="post_pt_root_id", version=7, ds="ds"):
    return {"table_name": table, "spec_name": spec, "partition_by": spec,
            "uri": f"gs://bucket/deltalake/{ds}/v{version}/{table}/{spec}",
            "spec": {"name": spec, "partition_by": spec, "n_partitions": 4},
            "row_count": 3, "bytes": 100, "files": 1, "columns": [{"name": "id", "type": "int64"}],
            "completed_at": "2026-11-11T00:00:00+00:00", "exporter_version": "5.37.0", "job_id": "j1"}


class TestRegister:
    def test_one_asset_per_lake(self, fake):
        result = cs.register_lakes("ds", 7, [row(spec="id"), row()])
        assert result == {"registered": [row(spec="id")["uri"], row()["uri"]], "failed": []}
        by_name = {a["name"]: a for a in fake.assets.values()}
        assert set(by_name) == {"synapses.id", "synapses.post_pt_root_id"}
        a = by_name["synapses.post_pt_root_id"]
        assert (a["format"], a["is_managed"], a["mat_version"], a["source"]) == ("delta", True, 7, "materialization")
        p = a["properties"]
        assert p["source"] == "materialization" and p["source_table"] == "synapses" and p["mat_version"] == 7
        assert p["partition_by"] == "post_pt_root_id" and p["spec"]["n_partitions"] == 4
        assert (p["row_count"], p["bytes"], p["files"]) == (3, 100, 1)
        assert p["index_uri"] == row()["uri"] + "/_cave_index/v1.arrows"

    def test_a_reexport_replaces_its_own_asset(self, fake):
        cs.register_lakes("ds", 7, [row()])
        [old] = fake.assets
        assert cs.register_lakes("ds", 7, [row()])["failed"] == []
        assert len(fake.assets) == 1 and old not in fake.assets

    def test_an_asset_registered_by_someone_else_is_left_alone(self, fake):
        fake.register_table("their_copy", row()["uri"], "delta", False, properties={"source": "user"})
        result = cs.register_lakes("ds", 7, [row()])
        assert result["failed"] == [row()["uri"]]
        assert [a["name"] for a in fake.assets.values()] == ["their_copy"]

    def test_failures_are_reported_not_raised(self, fake):
        with mock.patch.object(fake, "register_table", side_effect=RuntimeError("503")):
            assert cs.register_lakes("ds", 7, [row()]) == {"registered": [], "failed": [row()["uri"]]}

    def test_register_version_registers_the_complete_rows(self, fake):
        with mock.patch.object(cat, "list_exports", return_value=[row()]) as listed:
            cs.register_version("av", "ds", 7, table_name="synapses")
        assert listed.call_args.args == ("av", "ds", 7) and listed.call_args.kwargs == {"table_name": "synapses"}
        assert [a["name"] for a in fake.assets.values()] == ["synapses.post_pt_root_id"]

    def test_an_export_registers_only_its_own_lakes(self, fake):
        """An export beside an earlier one (another name_suffix) must not re-register the
        earlier export's lakes, which may have been deregistered on purpose."""
        ours, earlier = dict(row(spec="id_e2e"), job_id="j2"), row(spec="id__s8")
        with mock.patch.object(cat, "list_exports", return_value=[earlier, ours]):
            result = cs.register_version("av", "ds", 7, table_name="synapses", job_id="j2")
        assert result["registered"] == [ours["uri"]]
        assert [a["name"] for a in fake.assets.values()] == ["synapses.id_e2e"]

    def test_off_without_a_url(self, monkeypatch):
        monkeypatch.setattr(cs, "catalog_url", lambda: "")
        assert not cs.enabled()


class TestDeregister:
    def test_only_this_versions_lakes_go(self, fake):
        cs.register_lakes("ds", 7, [row(spec="id"), row()])
        cs.register_lakes("ds", 8, [row(version=8)])
        fake.register_table("synapses_mine", "gs://bucket/mine/ds/v7/x", "delta", False, mat_version=7)

        assert sorted(cs.deregister_version("ds", 7, dry_run=True)) == sorted([row(spec="id")["uri"], row()["uri"]])
        assert len(fake.assets) == 4

        assert sorted(cs.deregister_version("ds", 7)) == sorted([row(spec="id")["uri"], row()["uri"]])
        assert sorted(a["name"] for a in fake.assets.values()) == ["synapses.post_pt_root_id", "synapses_mine"]
        assert [a["mat_version"] for a in fake.assets.values() if a["source"] == "materialization"] == [8]


class TestCatalogUrl:
    """Where the service is: configured, explicitly off, or the in-cluster default when the
    cluster runs the catalog chart (its DNS name resolves)."""

    @pytest.fixture(autouse=True)
    def fresh(self, monkeypatch):
        monkeypatch.setattr(cs, "_resolved", {})
        monkeypatch.delenv("CATALOG_SERVICE_URL", raising=False)

    def lookups(self, monkeypatch, resolves: bool):
        import socket

        calls = []

        def getaddrinfo(host, port):
            calls.append(host)
            if not resolves:
                raise socket.gaierror(-2, "Name or service not known")
            return [("addr",)]

        monkeypatch.setattr(cs.socket, "getaddrinfo", getaddrinfo)
        return calls

    def test_configured(self, monkeypatch):
        monkeypatch.setenv("CATALOG_SERVICE_URL", "https://local.cave.com/catalog/")
        assert cs.catalog_url() == "https://local.cave.com/catalog" and cs.enabled()

    def test_explicitly_off(self, monkeypatch):
        calls = self.lookups(monkeypatch, resolves=True)
        monkeypatch.setenv("CATALOG_SERVICE_URL", "")
        assert cs.catalog_url() == "" and not cs.enabled() and not calls

    def test_unset_uses_the_in_cluster_service_when_deployed(self, monkeypatch):
        calls = self.lookups(monkeypatch, resolves=True)
        assert cs.catalog_url() == cs.DEFAULT_URL == "http://catalog-service.default.svc.cluster.local/catalog"
        assert cs.enabled()
        assert calls == ["catalog-service.default.svc.cluster.local"]  # looked up once, then cached
        cs.catalog_url()
        assert len(calls) == 1

    def test_unset_and_not_deployed_is_off(self, monkeypatch):
        self.lookups(monkeypatch, resolves=False)
        assert cs.catalog_url() == "" and not cs.enabled()

    def test_tests_never_look_for_a_service(self):
        from materializationengine.config import TestConfig

        assert TestConfig.CATALOG_SERVICE_URL == ""
