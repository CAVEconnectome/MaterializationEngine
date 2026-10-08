from unittest.mock import MagicMock
import sys

sys.modules["materializationengine.chunkedgraph_gateway"] = MagicMock()
from materializationengine.workflows.update_root_ids import (
    get_expired_root_ids_from_pcg,
    get_new_root_ids,
    get_supervoxel_id_queries,
)
import logging

import pytest


mocked_expired_root_id_data = [
    [20000000, 20000001],
    [20000002, 20000003],
    [20000004, 20000005],
    [20000006, 20000007],
    [20000008, 20000009],
]

mocked_supervoxel_chunks = [
    {
        "post_pt_supervoxel_id": 10000000,
        "pre_pt_supervoxel_id": 10000000,
        "id": 1,
        "post_pt_root_id": 20000000,
        "pre_pt_root_id": 20000000,
    },
    {
        "post_pt_supervoxel_id": 10000000,
        "pre_pt_supervoxel_id": 10000000,
        "id": 2,
        "post_pt_root_id": 20000000,
        "pre_pt_root_id": 20000000,
    },
    {
        "post_pt_supervoxel_id": 10000000,
        "pre_pt_supervoxel_id": 10000000,
        "id": 3,
        "post_pt_root_id": 20000000,
        "pre_pt_root_id": 20000000,
    },
]


class TestUpdateRootIds:
    def test_get_expired_root_ids(self, monkeypatch, mat_metadata):
        def mock_lookup_expire_root_ids(*args, **kwargs):
            return list(range(20000000, 20000010))

        monkeypatch.setattr(
            "materializationengine.workflows.update_root_ids.lookup_expired_root_ids",
            mock_lookup_expire_root_ids,
        )
        expired_root_ids = get_expired_root_ids_from_pcg(mat_metadata, 2)
        index = 0
        for root_id in expired_root_ids:
            logging.info(root_id)
            assert root_id == mocked_expired_root_id_data[index]
            index += 1

    def test_get_supervoxel_ids(self, annotation_data, mat_metadata):
        expired_roots = annotation_data["expired_root_ids"]

        supervoxel_ids = get_supervoxel_id_queries(expired_roots, mat_metadata)
        logging.info(supervoxel_ids)
        assert supervoxel_ids == [
            {
                "pre_pt_root_id": "SELECT test_synapse_table__test_pcg.id, test_synapse_table__test_pcg.pre_pt_root_id, test_synapse_table__test_pcg.pre_pt_supervoxel_id \n"
                "FROM test_synapse_table__test_pcg \n"
                "WHERE test_synapse_table__test_pcg.pre_pt_root_id IN (10000000000000000, 40000000000000000, 50000000000000000) OR test_synapse_table__test_pcg.pre_pt_root_id IS NULL"
            },
            {
                "post_pt_root_id": "SELECT test_synapse_table__test_pcg.id, test_synapse_table__test_pcg.post_pt_root_id, test_synapse_table__test_pcg.post_pt_supervoxel_id \n"
                "FROM test_synapse_table__test_pcg \n"
                "WHERE test_synapse_table__test_pcg.post_pt_root_id IN (10000000000000000, 40000000000000000, 50000000000000000) OR test_synapse_table__test_pcg.post_pt_root_id IS NULL"
            },
        ]

    def test_get_new_roots(self, monkeypatch, mat_metadata, annotation_data):
        def mock_lookup_new_root_ids(*args, **kwargs):
            return annotation_data["new_root_ids"]

        monkeypatch.setattr(
            "materializationengine.workflows.update_root_ids.lookup_new_root_ids",
            mock_lookup_new_root_ids,
        )

        supervoxel_chunk = annotation_data["segmentation_data"]
        new_roots = get_new_root_ids.s(supervoxel_chunk, mat_metadata).apply()
        assert new_roots.get() == "Number of rows updated: 3"

    @pytest.mark.parametrize("enabled", [True, False])
    def test_get_new_roots_records_old_and_new_roots_when_enabled(
        self, monkeypatch, mat_metadata, annotation_data, enabled
    ):
        from materializationengine.workflows import root_id_update_log

        monkeypatch.setattr(
            "materializationengine.workflows.update_root_ids.lookup_new_root_ids",
            lambda *args, **kwargs: annotation_data["new_root_ids"],
        )
        monkeypatch.setattr(root_id_update_log, "enabled", lambda md: enabled)
        recorded = []
        monkeypatch.setattr(
            root_id_update_log,
            "write_chunk",
            lambda md, table, task_id: recorded.append((table, task_id)),
        )
        supervoxel_chunk = annotation_data["segmentation_data"]
        old = [row["pre_pt_root_id"] for row in supervoxel_chunk]
        result = get_new_root_ids.s(supervoxel_chunk, mat_metadata).apply()
        assert result.get() == "Number of rows updated: 3"
        if not enabled:
            assert recorded == []
            return
        (table, task_id), = recorded
        assert task_id == result.id
        rows = table.to_pylist()
        assert [r["old_root_id"] for r in rows] == old
        assert [r["new_root_id"] for r in rows] == annotation_data["new_root_ids"]
        assert {r["root_column"] for r in rows} == {"pre_pt"}
