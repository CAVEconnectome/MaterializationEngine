"""Prefork celery workers must not share database connections opened in the parent.

create_app() opens a DynamicAnnotationDB client (init_staging_database) in the parent
process. If forked children inherit it, their queries interleave on one socket and read
each other's results: NoSuchColumnError on annotation_table_metadata.id during the
spatial lookup, or "server closed the connection unexpectedly".
"""

import multiprocessing
import sys
from unittest import mock

import pytest

from materializationengine import database
from materializationengine.database import (
    db_manager,
    dynamic_annotation_cache,
    reset_database_connections,
)

QUERIES_PER_CHILD = 200


def _query_metadata_repeatedly(database_name, table_name, expected, results):
    errors = []
    for _ in range(QUERIES_PER_CHILD):
        try:
            metadata = dynamic_annotation_cache.get_db(database_name).database.get_table_metadata(
                table_name
            )
            if metadata != expected:
                errors.append(f"wrong row: {metadata}")
        except Exception as e:
            errors.append(f"{type(e).__name__}: {e}")
    results.put(errors[:3] + [len(errors)])


class TestForkSafeDatabaseConnections:
    """Database clients are rebuilt per worker process instead of shared across a fork."""

    def test_reset_in_parent_closes_connections(self):
        client = mock.MagicMock()
        engine = mock.MagicMock()
        with mock.patch.dict(dynamic_annotation_cache._clients, {"db": client}, clear=True), \
                mock.patch.dict(db_manager._engines, {"db": engine}, clear=True):
            reset_database_connections(close_connections=True)
            assert dynamic_annotation_cache._clients == {}
            assert db_manager._engines == {}

        client._database._cached_session.close.assert_called_once()
        client._database.engine.dispose.assert_called_once()
        engine.dispose.assert_called_once()

    def test_reset_in_child_forgets_connections_without_closing_them(self):
        client = mock.MagicMock()
        engine = mock.MagicMock()
        with mock.patch.dict(dynamic_annotation_cache._clients, {"db": client}, clear=True), \
                mock.patch.dict(db_manager._engines, {"db": engine}, clear=True), \
                mock.patch.object(database, "_inherited_database_state", []) as inherited:
            reset_database_connections(close_connections=False)
            assert dynamic_annotation_cache._clients == {}
            assert db_manager._engines == {}
            # kept referenced so garbage collection never closes the shared sockets
            assert inherited == [({"db": client}, {"db": engine}, {})]

        client._database._cached_session.close.assert_not_called()
        client._database.engine.dispose.assert_not_called()
        engine.dispose.assert_not_called()

    def test_worker_signal_handlers_reset_connections(self, test_app):
        from materializationengine import celery_worker

        assert celery_worker.close_database_connections_before_fork in (
            celery_worker.worker_init._live_receivers(None)
        )
        assert celery_worker.reset_database_connections_after_fork in (
            celery_worker.worker_process_init._live_receivers(None)
        )

        with mock.patch.object(database, "reset_database_connections") as mock_reset:
            celery_worker.close_database_connections_before_fork()
            celery_worker.reset_database_connections_after_fork()

        assert mock_reset.call_args_list == [
            mock.call(close_connections=True),
            mock.call(close_connections=False),
        ]

    @pytest.mark.skipif(
        sys.platform != "linux", reason="needs fork; macOS crashes forking after framework init"
    )
    @pytest.mark.parametrize("close_in_parent", [True, False], ids=["parent_close", "child_reset"])
    def test_forked_children_query_concurrently_without_errors(
        self, test_app, mat_metadata, close_in_parent
    ):
        database_name = mat_metadata["aligned_volume"]
        table_name = mat_metadata["annotation_table_name"]
        # open a client and connection in the parent, as create_app does
        expected = dynamic_annotation_cache.get_db(database_name).database.get_table_metadata(
            table_name
        )
        assert expected

        if close_in_parent:
            reset_database_connections(close_connections=True)
            target = _query_metadata_repeatedly
        else:
            def target(*args):
                reset_database_connections(close_connections=False)
                _query_metadata_repeatedly(*args)

        context = multiprocessing.get_context("fork")
        results = context.Queue()
        children = [
            context.Process(target=target, args=(database_name, table_name, expected, results))
            for _ in range(2)
        ]
        for child in children:
            child.start()
        outcomes = [results.get(timeout=120) for _ in children]
        for child in children:
            child.join(30)

        assert [outcome[-1] for outcome in outcomes] == [0, 0], outcomes
        assert [child.exitcode for child in children] == [0, 0]
