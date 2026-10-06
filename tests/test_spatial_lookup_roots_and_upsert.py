"""Root id lookups zero only the supervoxels without a parent, and upserts lock rows in id order."""

import contextlib
from unittest import mock

import numpy as np
import pytest
from sqlalchemy.dialects import postgresql

from materializationengine.workflows import spatial_lookup


class _FakeRootExt:
    def __init__(self, roots, missing=()):
        self.roots = roots
        self.missing = set(missing)
        self.calls = []

    def get_roots(self, node_ids, time_stamp=None, fail_to_zero=False):
        self.calls.append(fail_to_zero)
        node_ids = np.asarray(node_ids)
        missing = [n for n in node_ids if int(n) in self.missing]
        if missing and not fail_to_zero:
            raise KeyError(np.uint64(missing[0]))
        return np.array([0 if int(n) in self.missing else self.roots[int(n)] for n in node_ids], dtype=np.uint64)


def _client(roots, missing=()):
    return mock.Mock(root_ext=_FakeRootExt(roots, missing))


class TestGetRootsZeroForMissing:
    def test_all_found(self):
        client = _client({1: 11, 2: 22})
        roots = spatial_lookup._get_roots_zero_for_missing(client, np.array([1, 2]), None, "pre_pt_supervoxel_id")
        assert roots.tolist() == [11, 22]
        assert client.root_ext.calls == [False]

    def test_missing_parent_zeroes_only_that_supervoxel(self):
        client = _client({1: 11, 3: 33}, missing={2})
        roots = spatial_lookup._get_roots_zero_for_missing(client, np.array([1, 2, 3]), None, "pre_pt_supervoxel_id")
        assert roots.tolist() == [11, 0, 33]
        assert client.root_ext.calls == [False, True]

    def test_single_supervoxel_gives_a_1d_array(self):
        roots = spatial_lookup._get_roots_zero_for_missing(_client({5: 55}), np.array([5]), None, "sv")
        assert roots.shape == (1,)

    def test_other_errors_propagate_so_the_task_retries(self):
        client = mock.Mock()
        client.root_ext.get_roots.side_effect = RuntimeError("bigtable unavailable")
        with pytest.raises(RuntimeError):
            spatial_lookup._get_roots_zero_for_missing(client, np.array([1]), None, "sv")


class TestInsertSegmentationDataOrder:
    def test_rows_are_upserted_in_id_order(self):
        mat_metadata = {
            "annotation_table_name": "synapse_order_test",
            "schema": "synapse",
            "pcg_table_name": "minnie3_v1",
            "database": "staging",
        }
        ids = [7, 3, 9, 1, 5]
        data = [
            {"id": i, "pre_pt_supervoxel_id": 100 + i, "pre_pt_root_id": 200 + i,
             "post_pt_supervoxel_id": 300 + i, "post_pt_root_id": 400 + i}
            for i in ids
        ]
        session = mock.MagicMock()
        session.execute.return_value.rowcount = len(ids)

        @contextlib.contextmanager
        def fake_scope(database):
            yield session

        with mock.patch.object(spatial_lookup.db_manager, "session_scope", fake_scope):
            assert spatial_lookup.insert_segmentation_data(data, mat_metadata) == len(ids)

        stmt = session.execute.call_args.args[0]
        params = stmt.compile(dialect=postgresql.dialect()).params
        upserted = [params[f"id_m{i}"] for i in range(len(ids))]
        assert upserted == sorted(ids)
