"""static/js/columnMatch.js: the wizard's suggested column mapping (run with node)."""

import json
import os
import shutil
import subprocess

import pytest

SCRIPT = os.path.join(os.path.dirname(__file__), "..", "static", "js", "columnMatch.js")
FIELDS = ["id"] + [
    f"{point}_position_{axis}" for point in ("pre_pt", "post_pt", "ctr_pt") for axis in "xyz"
] + ["size"]

pytestmark = pytest.mark.skipif(shutil.which("node") is None, reason="needs node")


def suggest(fields, columns, skip=None):
    code = (
        f"const m = require({json.dumps(os.path.abspath(SCRIPT))});"
        f"console.log(JSON.stringify(m.suggestColumnMapping({json.dumps(fields)},"
        f" {json.dumps(columns)}, {json.dumps(skip or [])})));"
    )
    return json.loads(subprocess.run(["node", "-e", code], capture_output=True, text=True, check=True).stdout)


class TestSuggestColumnMapping:
    def test_identical_names(self):
        columns = ["valid", "pre_pt_root_id", "id_partition"] + FIELDS
        assert suggest(FIELDS, columns) == {f: f for f in FIELDS}

    def test_short_names_match_by_component(self):
        columns = ["id", "pre_x", "pre_y", "pre_z", "post_x", "post_y", "post_z", "ctr_x", "ctr_y", "ctr_z", "size"]
        mapping = suggest(FIELDS, columns)
        assert mapping["pre_pt_position_x"] == "pre_x"
        assert mapping["post_pt_position_x"] == "post_x"  # not pre_x: "post" != "pre"
        assert mapping["ctr_pt_position_z"] == "ctr_z"
        assert len(set(mapping.values())) == len(mapping) == len(FIELDS)

    def test_camel_case_and_abbreviations(self):
        mapping = suggest(FIELDS, ["prePosX", "postPosY", "size_nm"])
        assert mapping == {"pre_pt_position_x": "prePosX", "post_pt_position_y": "postPosY", "size": "size_nm"}

    def test_poor_matches_are_not_suggested(self):
        # root ids are not positions; id_partition is not a unique id; a bare x is too vague
        assert suggest(FIELDS, ["id_partition", "pre_pt_root_id", "post_pt_root_id", "x", "notes"]) == {}

    def test_skipped_fields_are_left_alone(self):
        mapping = suggest(FIELDS, ["pre_x", "size"], skip=["size"])
        assert mapping == {"pre_pt_position_x": "pre_x"}
