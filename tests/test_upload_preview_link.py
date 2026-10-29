"""The upload preview link: the first rows of a proposed table as point layers over the
datastack, the points kept in the table's declared resolution and placed by the layer's
coordinate space, whatever the datastack's own resolution.

Rows are the first ones of minnie's synapses_pni_2.csv (4,4,40 nm voxels), expressed in
each declared resolution: every case must put each point at the same place in the
viewer's 4,4,40 space.
"""

import io
import json
from unittest import mock

import numpy as np
import pandas as pd
import pytest
from flask import Flask, g

from materializationengine.blueprints.upload import api
from materializationengine.blueprints.upload import preview_link as pl

MINNIE_RESOLUTION = [4.0, 4.0, 40.0]
MINNIE_ROWS = """id,post_pt_position_x,post_pt_position_y,post_pt_position_z,pre_pt_position_x,pre_pt_position_y,pre_pt_position_z,ctr_pt_position_x,ctr_pt_position_y,ctr_pt_position_z,size
332741118,280664,132120,22174,280658,132078,22177,280664,132070,22175,4440.0
332741119,280634,133858,22232,280648,133798,22229,280652,133864,22230,3856.0
332741120,280704,132096,22043,280722,131964,22043,280670,132062,22043,4604.0
332741121,280684,131064,22233,280714,131142,22228,280672,131090,22228,3168.0
332741122,280698,133438,22135,280704,133366,22131,280646,133388,22138,324.0
"""
POINTS = ("pre_pt", "post_pt", "ctr_pt")
IDENTITY = {
    "id": "id",
    "size": "size",
    **{f"{p}_position_{a}": f"{p}_position_{a}" for p in POINTS for a in "xyz"},
}


@pytest.fixture(autouse=True)
def no_capability_probe(monkeypatch):
    """nglui fetches the target Neuroglancer deployment's bundle to see what it
    supports; these states use nothing that depends on it."""
    monkeypatch.setenv("NGLUI_DISABLE_CAPABILITY_PROBE", "1")


class FakeSource:
    def __init__(self, text):
        self.data = text.encode()
        self.size = len(self.data)

    def read(self, start, end):
        return self.data[start:end]


def fake_client(resolution=MINNIE_RESOLUTION):
    client = mock.MagicMock()
    client.info.viewer_site.return_value = "https://spelunker.cave-explorer.org/"
    client.info.viewer_resolution.return_value = np.array(resolution)
    client.info.image_source.return_value = "precomputed://gs://minnie/em"
    client.info.segmentation_source.return_value = "graphene://https://minnie/segmentation/table/minnie3_v1"
    client.info.get_datastack_info.return_value = {"skeleton_source": None}
    client.state.upload_state_json.return_value = 1234
    client.state.build_neuroglancer_url.side_effect = (
        lambda state_id, target_url: f"{target_url}#!middleauth+https://global/nglstate/api/v1/{state_id}"
    )
    return client


def minnie_rows_in(resolution):
    """The minnie rows with their coordinates in the given resolution's units."""
    df = pd.read_csv(io.StringIO(MINNIE_ROWS))
    for p in POINTS:
        cols = [f"{p}_position_{a}" for a in "xyz"]
        df[cols] = df[cols].to_numpy() * np.array(MINNIE_RESOLUTION) / np.array(resolution)
    return df.astype(str)


def layers_for(rows):
    return pl.point_layers(rows, pl.spatial_points("synapse", IDENTITY), "id")


def state_dict(rows, declared, client=None):
    state = pl.build_preview_state(client or fake_client(), layers_for(rows), declared)
    return state, state.to_dict()


def viewer_voxels(layer, viewer_resolution):
    """Where Neuroglancer draws a layer's points, in viewer voxels: each stored point
    times the layer's output dimensions, over the viewer's."""
    out = layer["source"][0]["transform"]["outputDimensions"]
    scale = np.array([out[a][0] * 1e9 for a in "xyz"]) / np.array(viewer_resolution)
    return np.array([a["point"] for a in layer["annotations"]]) * scale


CASES = {
    "matches the datastack": [4, 4, 40],
    "nanometers": [1, 1, 1],
    "coarser xy": [8, 8, 40],
    "finer z": [2, 2, 20],
    "non-integer": [3.5, 3.5, 35],
}


class TestReadFirstRows:
    def test_limits_rows_and_drops_a_cut_off_last_line(self):
        source = FakeSource(MINNIE_ROWS + "332741124,280676,13")
        with mock.patch.object(pl, "PREVIEW_BYTES", len(MINNIE_ROWS) + 10):
            rows = pl.read_first_rows("gs://b/synapses.csv", source=source, rows=3)
        assert list(rows["id"]) == ["332741118", "332741119", "332741120"]

    def test_a_short_file_whole(self):
        rows = pl.read_first_rows("gs://b/synapses.csv", source=FakeSource(MINNIE_ROWS))
        assert len(rows) == 5 and rows.columns[0] == "id"


class TestPointLayers:
    def test_one_layer_per_spatial_field_of_the_schema(self):
        layers = layers_for(minnie_rows_in(MINNIE_RESOLUTION))
        assert sorted(layers) == sorted(POINTS)
        assert list(layers["pre_pt"]["description"]) == [
            "332741118", "332741119", "332741120", "332741121", "332741122"
        ]

    def test_mapped_columns_and_a_combined_column(self):
        rows = minnie_rows_in(MINNIE_RESOLUTION).rename(
            columns={"pre_pt_position_x": "px", "pre_pt_position_y": "py", "pre_pt_position_z": "pz"}
        )
        rows["post"] = [
            f"[{x}, {y}, {z}]"
            for x, y, z in rows[["post_pt_position_x", "post_pt_position_y", "post_pt_position_z"]].to_numpy()
        ]
        mapping = {
            **IDENTITY,
            "pre_pt_position_x": "px", "pre_pt_position_y": "py", "pre_pt_position_z": "pz",
            "post_pt_position": "post",
        }
        for a in "xyz":
            mapping.pop(f"post_pt_position_{a}")
        layers = pl.point_layers(rows, pl.spatial_points("synapse", mapping), "id")
        assert layers["pre_pt"][["x", "y", "z"]].iloc[0].tolist() == [280658, 132078, 22177]
        assert layers["post_pt"][["x", "y", "z"]].iloc[0].tolist() == [280664, 132120, 22174]

    def test_rows_without_a_point_are_left_out(self):
        rows = minnie_rows_in(MINNIE_RESOLUTION)
        rows.loc[0, "ctr_pt_position_x"] = ""
        assert len(layers_for(rows)["ctr_pt"]) == 4

    def test_unmapped_points_give_no_layer(self):
        rows = minnie_rows_in(MINNIE_RESOLUTION).drop(columns=["ctr_pt_position_x"])
        assert "ctr_pt" not in layers_for(rows)


class TestResolutions:
    @pytest.mark.parametrize("declared", CASES.values(), ids=CASES.keys())
    def test_points_land_on_the_synapses_in_the_viewer(self, declared):
        _, st = state_dict(minnie_rows_in(declared), declared)
        # the viewer stays in the datastack's resolution
        assert [st["dimensions"][a][0] * 1e9 for a in "xyz"] == pytest.approx(MINNIE_RESOLUTION)
        truth = pd.read_csv(io.StringIO(MINNIE_ROWS))
        for p in POINTS:
            layer = next(L for L in st["layers"] if L["name"] == p)
            out = layer["source"][0]["transform"]["outputDimensions"]
            # the layer declares the table's resolution...
            assert [out[a][0] * 1e9 for a in "xyz"] == pytest.approx(declared)
            # ...its points are the uploaded numbers, not rescaled...
            stored = np.array([a["point"] for a in layer["annotations"]])
            uploaded = minnie_rows_in(declared)[[f"{p}_position_{a}" for a in "xyz"]].astype(float).to_numpy()
            np.testing.assert_allclose(stored, uploaded, rtol=1e-6)
            # ...and they are drawn at the synapses' 4,4,40 voxels
            np.testing.assert_allclose(
                viewer_voxels(layer, MINNIE_RESOLUTION),
                truth[[f"{p}_position_{a}" for a in "xyz"]].to_numpy(),
                rtol=1e-6,
            )

    @pytest.mark.parametrize("declared", CASES.values(), ids=CASES.keys())
    def test_the_view_opens_on_the_first_rows(self, declared):
        _, st = state_dict(minnie_rows_in(declared), declared)
        first_ctr = pd.read_csv(io.StringIO(MINNIE_ROWS)).iloc[0][
            ["ctr_pt_position_x", "ctr_pt_position_y", "ctr_pt_position_z"]
        ].to_numpy(dtype=float)
        np.testing.assert_allclose(st["position"], first_ctr, rtol=1e-6)

    def test_wrong_declared_resolution_puts_points_elsewhere(self):
        """The mistake the preview is for: 4,4,40 coordinates declared as nanometers."""
        _, st = state_dict(minnie_rows_in(MINNIE_RESOLUTION), [1, 1, 1])
        layer = next(L for L in st["layers"] if L["name"] == "pre_pt")
        drawn = viewer_voxels(layer, MINNIE_RESOLUTION)[0]
        np.testing.assert_allclose(drawn, [280658 / 4, 132078 / 4, 22177 / 40], rtol=1e-6)

    def test_a_datastack_with_another_resolution(self):
        """Same rows on a datastack whose viewer is in 8,8,40: still on the synapses."""
        declared = [1, 1, 1]
        _, st = state_dict(minnie_rows_in(declared), declared, client=fake_client([8, 8, 40]))
        layer = next(L for L in st["layers"] if L["name"] == "pre_pt")
        nm = np.array([280658, 132078, 22177]) * np.array(MINNIE_RESOLUTION)
        np.testing.assert_allclose(viewer_voxels(layer, [8, 8, 40])[0], nm / [8, 8, 40], rtol=1e-6)

    def test_link_is_shortened_through_the_state_service(self):
        client = fake_client()
        state, _ = state_dict(minnie_rows_in([1, 1, 1]), [1, 1, 1], client=client)
        url = state.to_link_shortener(client)
        assert url == "https://spelunker.cave-explorer.org/#!middleauth+https://global/nglstate/api/v1/1234"
        uploaded = client.state.upload_state_json.call_args.args[0]
        assert {L["name"] for L in uploaded["layers"]} >= set(POINTS)


METADATA = {
    "schema_type": "synapse",
    "datastack_name": "minnie65_phase3_v1",
    "table_name": "synapses_test",
    "description": "d",
    "voxel_resolution_nm_x": 1,
    "voxel_resolution_nm_y": 1,
    "voxel_resolution_nm_z": 1,
    "write_permission": "PRIVATE",
    "read_permission": "PUBLIC",
}
BODY = {"filename": "synapses.csv", "columnMapping": IDENTITY, "ignoredColumns": [], "metadata": METADATA}


@pytest.fixture
def app():
    app = Flask(__name__)
    app.config.update(MATERIALIZATION_UPLOAD_BUCKET_PATH="bucket", GLOBAL_SERVER_URL="https://global")
    return app


def call_preview(app, body=BODY, rows_csv=None, auth_user=None, edit=True):
    import inspect

    csv_text = rows_csv if rows_csv is not None else minnie_rows_in([1, 1, 1]).to_csv(index=False)
    client = fake_client()
    with app.test_request_context(json=body), \
            mock.patch.object(api, "is_auth_disabled", return_value=False), \
            mock.patch.object(api, "_has_datastack_permission", return_value=edit), \
            mock.patch.object(api, "GCSCsvProcessor") as gcs, \
            mock.patch("caveclient.CAVEclient", return_value=client):
        gcs.return_value.source.return_value = FakeSource(csv_text)
        g.auth_user = auth_user or {"id": 1}
        response = inspect.unwrap(api.preview_link)()
        response, code = response if isinstance(response, tuple) else (response, response.status_code)
        return json.loads(response.get_data()), code, gcs


class TestEndpoint:
    def test_returns_a_short_link_and_what_it_shows(self, app):
        body, code, gcs = call_preview(app)
        assert code == 200, body
        assert body["url"].endswith("/nglstate/api/v1/1234")
        assert body["data_resolution"] == [1, 1, 1]
        assert body["viewer_resolution"] == MINNIE_RESOLUTION
        assert {L["name"]: L["points"] for L in body["layers"]} == {p: 5 for p in POINTS}
        gcs.return_value.source.assert_called_once_with("gs://bucket/synapses.csv")

    def test_reads_a_source_in_place(self, app):
        _, code, gcs = call_preview(app, body={**BODY, "sourceUri": "gs://mat_dbs/synapses_pni_2.csv"})
        assert code == 200
        gcs.return_value.source.assert_called_once_with("gs://mat_dbs/synapses_pni_2.csv")

    def test_needs_edit_on_the_datastack(self, app):
        body, code, gcs = call_preview(app, edit=False)
        assert code == 403 and "edit" in body["message"]
        gcs.assert_not_called()

    def test_no_points_in_the_mapped_columns(self, app):
        mapping = {"id": "id"}
        body, code, _ = call_preview(app, body={**BODY, "columnMapping": mapping},
                                     rows_csv="id,a\n1,2\n")
        assert code == 400 and "point" in body["message"]

    def test_bad_body(self, app):
        _, code, _ = call_preview(app, body={"filename": "x.csv"})
        assert code == 400


def link_state(url):
    from urllib.parse import unquote

    return json.loads(unquote(url.split("#!", 1)[1]))


def source_urls(layer):
    return [s["url"] for s in layer["source"]]


class TestViewsLinks:
    """The table view links, ported from nglui 3's StateBuilder."""

    def links(self, make):
        from materializationengine import views

        client = fake_client()
        client.info.get_datastack_info.return_value = {"skeleton_source": "precomputed://gs://minnie/skel"}
        with mock.patch.object(views, "url_for", return_value="https://minnie/materialize/table/info"), \
                mock.patch.object(views, "get_config_param", return_value=False):
            url = make(views, client)
        assert url.startswith("https://spelunker.cave-explorer.org")
        state = link_state(url)
        assert [state["dimensions"][a][0] * 1e9 for a in "xyz"] == pytest.approx(MINNIE_RESOLUTION)
        return {L["name"]: L for L in state["layers"]}

    def test_seg_prop_link_has_the_segment_properties_source(self):
        layers = self.links(lambda views, c: views.make_seg_prop_ng_link("minnie65_phase3_v1", "synapses", 1953, c))
        assert source_urls(layers["img"]) == ["precomputed://gs://minnie/em"]
        assert source_urls(layers["seg"]) == [
            "graphene://middleauth+https://minnie/segmentation/table/minnie3_v1",
            "precomputed://middleauth+https://minnie/materialize/table",
            "precomputed://gs://minnie/skel",
        ]

    def test_precomputed_annotation_link_adds_the_table_layer(self):
        layers = self.links(lambda views, c: views.make_precomputed_annotation_link("minnie65_phase3_v1", "synapses", c))
        assert source_urls(layers["seg"]) == [
            "graphene://middleauth+https://minnie/segmentation/table/minnie3_v1",
            "precomputed://gs://minnie/skel",
        ]
        anno = layers["synapses"]
        assert anno["type"] == "annotation" and anno["annotationColor"] == "#ffffff"
        assert source_urls(anno) == ["precomputed://middleauth+https://minnie/materialize/table"]
