from unittest.mock import MagicMock
import sys

sys.modules["materializationengine.chunkedgraph_gateway"] = MagicMock()
from materializationengine.workflows.update_root_ids import (
    LOOKUP_ALL_CHUNK_ROWS,
    MISSING_ROOTS,
    get_expired_root_ids_from_pcg,
    get_new_root_ids,
    get_supervoxel_id_queries,
    update_root_ids_workflow,
)
import logging
from unittest import mock

import numpy as np


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

    def test_expired_root_ids_are_deduplicated_before_chunking(self, monkeypatch, mat_metadata):
        # the chunkedgraph can list one old root twice; it must land in exactly one chunk
        roots = [20000003, 20000001, 20000003, 20000002, 20000001, 20000004]
        monkeypatch.setattr(
            "materializationengine.workflows.update_root_ids.lookup_expired_root_ids",
            lambda *args, **kwargs: np.array(roots),
        )
        chunks = list(get_expired_root_ids_from_pcg(mat_metadata, 2))
        flat = [r for chunk in chunks for r in chunk]
        assert sorted(flat) == [20000001, 20000002, 20000003, 20000004]
        assert len(flat) == len(set(flat))

    def test_get_supervoxel_ids(self, annotation_data, mat_metadata):
        expired_roots = annotation_data["expired_root_ids"]

        supervoxel_ids = get_supervoxel_id_queries(expired_roots, mat_metadata)
        logging.info(supervoxel_ids)
        assert supervoxel_ids == [
            {
                "pre_pt_root_id": "SELECT test_synapse_table__test_pcg.id, test_synapse_table__test_pcg.pre_pt_root_id, test_synapse_table__test_pcg.pre_pt_supervoxel_id \n"
                "FROM test_synapse_table__test_pcg \n"
                "WHERE test_synapse_table__test_pcg.pre_pt_root_id IN (10000000000000000, 40000000000000000, 50000000000000000)"
            },
            {
                "post_pt_root_id": "SELECT test_synapse_table__test_pcg.id, test_synapse_table__test_pcg.post_pt_root_id, test_synapse_table__test_pcg.post_pt_supervoxel_id \n"
                "FROM test_synapse_table__test_pcg \n"
                "WHERE test_synapse_table__test_pcg.post_pt_root_id IN (10000000000000000, 40000000000000000, 50000000000000000)"
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

    def test_missing_roots_chunk_selects_null_roots_that_have_a_supervoxel(self, mat_metadata):
        (pre, post) = get_supervoxel_id_queries(MISSING_ROOTS, mat_metadata)
        assert pre["pre_pt_root_id"].endswith(
            "WHERE test_synapse_table__test_pcg.pre_pt_root_id IS NULL "
            "AND test_synapse_table__test_pcg.pre_pt_supervoxel_id IS NOT NULL")

    def test_lookup_all_chunks_are_id_ranges_and_the_last_runs_to_the_end(self, mat_metadata):
        metadata = dict(mat_metadata, lookup_all_root_ids=True)
        (pre, _) = get_supervoxel_id_queries([1, 500], metadata)
        assert pre["pre_pt_root_id"].endswith("WHERE test_synapse_table__test_pcg.id BETWEEN 1 AND 500")
        (pre, _) = get_supervoxel_id_queries([501, None], metadata)
        assert pre["pre_pt_root_id"].endswith("WHERE test_synapse_table__test_pcg.id >= 501")

    def test_only_rows_whose_root_changed_are_written(self, mat_metadata):
        rows = [
            {"id": 1, "pre_pt_root_id": 101, "pre_pt_supervoxel_id": 11},   # unchanged
            {"id": 2, "pre_pt_root_id": 102, "pre_pt_supervoxel_id": 12},   # changed
            {"id": 3, "pre_pt_root_id": None, "pre_pt_supervoxel_id": 13},  # was NULL
        ]
        session = mock.MagicMock()
        scope = mock.MagicMock()
        scope.return_value.__enter__.return_value = session
        with mock.patch("materializationengine.workflows.update_root_ids.lookup_new_root_ids",
                        return_value=np.array([101, 202, 303])), \
                mock.patch("materializationengine.workflows.update_root_ids.db_manager.session_scope", scope), \
                mock.patch("materializationengine.workflows.update_root_ids.create_segmentation_model"):
            result = get_new_root_ids.s(rows, mat_metadata).apply().get()
        assert result == "Number of rows updated: 2"
        written = session.bulk_update_mappings.call_args.args[1]
        assert [(r["id"], int(r["pre_pt_root_id"])) for r in written] == [(2, 202), (3, 303)]
        assert all("pre_pt_supervoxel_id" not in r for r in written)


WORKFLOW = "materializationengine.workflows.update_root_ids"


class TestUpdateRootIdsWorkflow:
    def _run(self, mat_metadata, expired=None, model_chunks=None):
        sent = []
        with mock.patch(f"{WORKFLOW}.get_expired_root_ids_from_pcg", return_value=expired), \
                mock.patch(f"{WORKFLOW}.generate_chunked_model_ids", return_value=model_chunks) as chunker, \
                mock.patch(f"{WORKFLOW}.run_with_limit",
                           side_effect=lambda sigs, limit: sent.extend(s.args[0] for s in sigs) or len(sent)), \
                mock.patch(f"{WORKFLOW}.update_metadata") as update_metadata, \
                mock.patch(f"{WORKFLOW}.monitor_task_states") as monitor:
            update_root_ids_workflow(mat_metadata)
        return sent, chunker, update_metadata, monitor

    def test_expired_roots_then_one_chunk_of_null_roots(self, mat_metadata):
        sent, _, update_metadata, monitor = self._run(mat_metadata, expired=iter([[1, 2], [3]]))
        assert sent == [[1, 2], [3], MISSING_ROOTS]
        update_metadata.si.assert_called_once()
        monitor.assert_called_once()

    def test_null_roots_are_filled_even_when_nothing_expired(self, mat_metadata):
        sent, _, update_metadata, _ = self._run(mat_metadata, expired=None)
        assert sent == [MISSING_ROOTS]
        update_metadata.si.assert_called_once()

    def test_lookup_all_chunks_the_whole_table_in_large_chunks(self, mat_metadata):
        metadata = dict(mat_metadata, lookup_all_root_ids=True)
        sent, chunker, _, _ = self._run(metadata, model_chunks=iter([[1, 50001], [50001, None]]))
        assert sent == [[1, 50001], [50001, None]]  # no separate NULL chunk: every row is looked up
        assert chunker.call_args.args[0]["chunk_size"] == LOOKUP_ALL_CHUNK_ROWS

    def test_a_failed_chunk_leaves_last_updated_alone(self, mat_metadata):
        import pytest

        with mock.patch(f"{WORKFLOW}.get_expired_root_ids_from_pcg", return_value=iter([[1]])), \
                mock.patch(f"{WORKFLOW}.run_with_limit", side_effect=Exception("chunk failed")), \
                mock.patch(f"{WORKFLOW}.update_metadata") as update_metadata, \
                pytest.raises(Exception, match="chunk failed"):
            update_root_ids_workflow(mat_metadata)
        update_metadata.si.assert_not_called()
