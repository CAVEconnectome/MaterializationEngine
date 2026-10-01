# Mock pychunkgraph imports and cloudvolume before importing
# the tasks
import sys
import datetime
import logging
from collections import namedtuple
from contextlib import contextmanager
from unittest import mock

sys.modules["materializationengine.chunkedgraph_gateway"] = mock.MagicMock()
sys.modules["cloudvolume"] = mock.MagicMock()

import numpy as np
from materializationengine.workflows.ingest_new_annotations import (
    create_missing_segmentation_table,
    get_annotations_with_missing_supervoxel_ids,
    find_missing_root_ids_workflow,
    get_cloudvolume_supervoxel_ids,
    get_ids_with_missing_roots,
    get_new_root_ids,
    get_sql_supervoxel_ids_chunks,
    insert_segmentation_data,
    table_modified_since_last_update,
)
from numpy import nan
from sqlalchemy.orm import Query


missing_segmentation_data = {
    "post_pt_supervoxel_id": [nan],
    "pre_pt_supervoxel_id": [nan],
    "post_pt_position": [
        [73000, 83000, 93000],
    ],
    "pre_pt_position": [
        [13000, 23000, 33000],
    ],
    "id": [4],
}


mocked_supervoxel_data = {
    "post_pt_supervoxel_id": [10000000],
    "pre_pt_supervoxel_id": [10000000],
    "post_pt_position": [
        [73000, 83000, 93000],
    ],
    "pre_pt_position": [
        [13000, 23000, 33000],
    ],
    "id": [4],
}


mocked_root_id_data = [
    {
        "post_pt_supervoxel_id": 10000000,
        "pre_pt_supervoxel_id": 10000000,
        "id": 1,
        "post_pt_root_id": 20000000000000000,
        "pre_pt_root_id": 10000000000000000,
    },
    {
        "post_pt_supervoxel_id": 10000000,
        "pre_pt_supervoxel_id": 10000000,
        "id": 2,
        "post_pt_root_id": 40000000000000000,
        "pre_pt_root_id": 30000000000000000,
    },
    {
        "post_pt_supervoxel_id": 10000000,
        "pre_pt_supervoxel_id": 10000000,
        "id": 3,
        "post_pt_root_id": 60000000000000000,
        "pre_pt_root_id": 50000000000000000,
    },
]


class TestIngestMissingAnnotations:
    def test_create_missing_segmentation_table(self, mat_metadata, db_client):
        table_metadata = create_missing_segmentation_table.s(mat_metadata).apply()

        __, engine = db_client

        seg_table_exists = engine.dialect.has_table(
            engine.connect(), "test_synapse_table__test_pcg"
        )
        return_val = table_metadata.get()
        assert return_val is True
        assert seg_table_exists is True

    def test_get_annotations_with_missing_supervoxel_ids(self, mat_metadata):
        id_chunk_range = [1, 5]
        annotations = get_annotations_with_missing_supervoxel_ids(
            mat_metadata, id_chunk_range
        )
        logging.info(annotations)
        assert annotations == missing_segmentation_data

    @mock.patch(
        "materializationengine.workflows.ingest_new_annotations.cloudvolume.CloudVolume"
    )
    def test_get_cloudvolume_supervoxel_ids(self, mock_cv, mat_metadata):
        mock_cv.return_value = True

        with mock.patch(
            "materializationengine.workflows.ingest_new_annotations.get_sv_id"
        ) as mock_get_sv_id:
            mock_get_sv_id.return_value = np.ndarray(
                (1,), buffer=np.array([10000000]), dtype=int
            )
            supervoxel_data = get_cloudvolume_supervoxel_ids(
                missing_segmentation_data, mat_metadata
            )
        assert supervoxel_data == mocked_supervoxel_data

    @mock.patch(
        "materializationengine.workflows.ingest_new_annotations.chunkedgraph_cache.init_pcg"
    )
    def test_get_new_root_ids(self, mock_chunkgraph, mat_metadata, annotation_data):
        mock_chunkgraph.return_value = True

        with mock.patch(
            "materializationengine.workflows.ingest_new_annotations.get_root_ids"
        ) as mock_get_roots:
            mock_get_roots.return_value = np.ndarray(
                (1,), buffer=np.array([20000000]), dtype=int
            )
            root_ids = get_new_root_ids(mocked_supervoxel_data, mat_metadata)
        assert root_ids == [
            {
                "post_pt_supervoxel_id": 10000000,
                "pre_pt_supervoxel_id": 10000000,
                "post_pt_root_id": 20000000,
                "pre_pt_root_id": 20000000,
                "id": 4,
            }
        ]

    def test_insert_segmentation_data(self, test_app, annotation_data, mat_metadata):
        segmentation_data = annotation_data["new_segmentation_data"]
        num_of_rows = insert_segmentation_data(segmentation_data, mat_metadata)
        assert num_of_rows == {"Segmentation data inserted": 1}

    def test_get_sql_supervoxel_ids(self, test_app, mat_metadata):
        id_chunk_range = [1, 4]
        supervoxel_ids = get_sql_supervoxel_ids_chunks(id_chunk_range, mat_metadata)
        logging.info(supervoxel_ids)
        assert supervoxel_ids == {
            "id": [1, 2, 3, 4],
            "pre_pt_supervoxel_id": [10000000, 30000000, 50000000, 10000000],
            "post_pt_supervoxel_id": [20000000, 40000000, 60000000, 20000000],
        }


def _mock_session_scope(rows):
    """Stand-in for db_manager.session_scope whose queries return ``rows``."""
    session = mock.MagicMock()
    session.query.return_value.filter.return_value = rows

    @contextmanager
    def session_scope(database_name):
        yield session

    return session_scope


class TestLookupMissingRootIds:
    """Rows posted through the API have a supervoxel_id but a NULL root_id, and the
    periodic workflows must fill those in whether or not any root ids have expired."""

    def test_get_ids_with_missing_roots_requires_supervoxel(self, mat_metadata):
        session = mock.MagicMock()
        session.query.side_effect = lambda *entities: Query(entities)

        @contextmanager
        def session_scope(database_name):
            yield session

        with mock.patch(
            "materializationengine.workflows.ingest_new_annotations.db_manager.session_scope",
            session_scope,
        ):
            stmt = get_ids_with_missing_roots(mat_metadata)
        sql = " ".join(str(stmt).split())
        assert (
            "pre_pt_root_id IS NULL AND test_synapse_table__test_pcg.pre_pt_supervoxel_id IS NOT NULL"
            in sql
        )
        assert (
            "post_pt_root_id IS NULL AND test_synapse_table__test_pcg.post_pt_supervoxel_id IS NOT NULL"
            in sql
        )
        assert " OR " in sql

    @mock.patch(
        "materializationengine.workflows.ingest_new_annotations.chunkedgraph_cache.init_pcg"
    )
    def test_get_new_root_ids_fills_each_missing_column(
        self, mock_chunkgraph, mat_metadata
    ):
        Row = namedtuple("Row", ["pre_pt_root_id", "post_pt_root_id", "id"])
        current_roots = [
            Row(pre_pt_root_id=111, post_pt_root_id=None, id=1),  # only post missing
            Row(pre_pt_root_id=None, post_pt_root_id=222, id=2),  # only pre missing
            Row(pre_pt_root_id=333, post_pt_root_id=444, id=3),  # nothing missing
        ]
        supervoxel_data = {
            "id": [1, 2, 3],
            "pre_pt_supervoxel_id": [10, 20, 30],
            "post_pt_supervoxel_id": [11, 21, 31],
        }

        with mock.patch(
            "materializationengine.workflows.ingest_new_annotations.db_manager.session_scope",
            _mock_session_scope(current_roots),
        ), mock.patch(
            "materializationengine.workflows.ingest_new_annotations.get_root_ids",
            side_effect=lambda cg, data, ts: np.asarray(data) * 1000,
        ) as mock_get_roots:
            root_ids = get_new_root_ids(supervoxel_data, mat_metadata)

        by_id = {row["id"]: row for row in root_ids}
        assert by_id[1]["pre_pt_root_id"] == 111
        assert by_id[1]["post_pt_root_id"] == 11000
        assert by_id[2]["pre_pt_root_id"] == 20000
        assert by_id[2]["post_pt_root_id"] == 222
        assert by_id[3]["pre_pt_root_id"] == 333
        assert by_id[3]["post_pt_root_id"] == 444
        looked_up = sorted(
            sv for call in mock_get_roots.call_args_list for sv in list(call.args[1])
        )
        assert looked_up == [11, 20]

    @mock.patch(
        "materializationengine.workflows.ingest_new_annotations.chunkedgraph_cache.init_pcg"
    )
    def test_get_new_root_ids_skips_missing_supervoxel(
        self, mock_chunkgraph, mat_metadata
    ):
        Row = namedtuple("Row", ["pre_pt_root_id", "post_pt_root_id", "id"])
        current_roots = [Row(pre_pt_root_id=None, post_pt_root_id=None, id=1)]
        supervoxel_data = {
            "id": [1],
            "pre_pt_supervoxel_id": [10],
            "post_pt_supervoxel_id": [nan],
        }

        with mock.patch(
            "materializationengine.workflows.ingest_new_annotations.db_manager.session_scope",
            _mock_session_scope(current_roots),
        ), mock.patch(
            "materializationengine.workflows.ingest_new_annotations.get_root_ids",
            side_effect=lambda cg, data, ts: np.asarray(data) * 1000,
        ) as mock_get_roots:
            root_ids = get_new_root_ids(supervoxel_data, mat_metadata)

        assert root_ids[0]["pre_pt_root_id"] == 10000
        assert root_ids[0]["post_pt_root_id"] is None
        assert mock_get_roots.call_count == 1

    def test_get_new_root_ids_accepts_timestamp_without_microseconds(
        self, mat_metadata
    ):
        metadata = dict(mat_metadata, materialization_time_stamp="2026-09-27 08:11:22")
        with mock.patch(
            "materializationengine.workflows.ingest_new_annotations.db_manager.session_scope",
            _mock_session_scope([]),
        ), mock.patch(
            "materializationengine.workflows.ingest_new_annotations.chunkedgraph_cache.init_pcg"
        ), mock.patch(
            "materializationengine.workflows.ingest_new_annotations.get_root_ids",
            return_value=np.array([5]),
        ) as mock_get_roots:
            get_new_root_ids(
                {"id": [1], "pre_pt_supervoxel_id": [10], "post_pt_supervoxel_id": [nan]},
                metadata,
            )
        assert mock_get_roots.call_args.args[2] == datetime.datetime(2026, 9, 27, 8, 11, 22)

    def test_unmodified_table_is_skipped(self, mat_metadata):
        # e.g. a synapse table untouched since 2022 must not be scanned every hour
        metadata = dict(
            mat_metadata,
            last_modified_time_stamp="2022-10-25 19:24:28.559914",
            last_updated_time_stamp="2026-09-28 08:10:43.464231",
        )
        assert table_modified_since_last_update(metadata) is False
        with mock.patch(
            "materializationengine.workflows.ingest_new_annotations.get_ids_with_missing_roots"
        ) as mock_query:
            find_missing_root_ids_workflow(metadata)
        mock_query.assert_not_called()

    def test_modified_table_is_scanned(self, mat_metadata):
        # the 2026-09-26 proofreading_status_and_strategy posts, seen by the next hourly run
        metadata = dict(
            mat_metadata,
            last_modified_time_stamp="2026-09-26 20:59:16.894085",
            last_updated_time_stamp="2026-09-26 21:03:22.719703",
        )
        assert table_modified_since_last_update(metadata) is True
        with mock.patch(
            "materializationengine.workflows.ingest_new_annotations.get_ids_with_missing_roots"
        ) as mock_query, mock.patch(
            "materializationengine.workflows.ingest_new_annotations.batch_missing_root_ids_query",
            return_value=[],
        ), mock.patch(
            "materializationengine.workflows.ingest_new_annotations.monitor_task_states"
        ):
            find_missing_root_ids_workflow(metadata)
        mock_query.assert_called_once()

    def test_modification_within_lookback_is_scanned(self, mat_metadata):
        # a post just before the last update may have had its supervoxel row
        # written after that update's scan ran
        metadata = dict(
            mat_metadata,
            last_modified_time_stamp="2026-09-26 19:22:00",
            last_updated_time_stamp="2026-09-26 23:03:22.106146",
        )
        assert table_modified_since_last_update(metadata) is True

    def test_missing_timestamps_are_scanned(self, mat_metadata):
        metadata = dict(
            mat_metadata, last_modified_time_stamp=None, last_updated_time_stamp=None
        )
        assert table_modified_since_last_update(metadata) is True
