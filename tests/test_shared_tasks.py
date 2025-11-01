import datetime
import logging

from dynamicannotationdb.models import AnalysisVersion
from emannotationschemas.models import make_annotation_model

from materializationengine.index_manager import IndexCache
from materializationengine.shared_tasks import (
    add_index,
    chunk_ids,
    collect_data,
    fin,
    generate_chunked_model_ids,
    get_materialization_info,
    query_id_range,
    update_metadata,
)

index_client = IndexCache()


class TestSharedTasks:
    def test_generate_chunked_model_ids(self, mat_metadata):
        anno_id_chunks = generate_chunked_model_ids(mat_metadata)
        logging.info(anno_id_chunks)

        assert next(anno_id_chunks) == [1, 3]
        assert next(anno_id_chunks) == [3, None]

    def test_fin(self):
        result = fin.s().apply()
        assert result.get() == True

    def test_get_materialization_info(self):
        datastack_info = {
            "datastack": "test_datastack",
            "aligned_volume": {"name": "test_aligned_volume"},
            "segmentation_source": "graphene://https://fake-daf.com/segmentation/table/test_pcg",
        }
        analysis_version = 1
        materialization_time_stamp = datetime.datetime.utcnow()
        mat_info = get_materialization_info(
            datastack_info, analysis_version, materialization_time_stamp
        )
        assert isinstance(mat_info, list)
        assert len(mat_info) == 1
        assert mat_info[0]["datastack"] == "test_datastack"
        assert mat_info[0]["aligned_volume"] == "test_aligned_volume"
        assert mat_info[0]["segmentation_source"] == "graphene://https://fake-daf.com/segmentation/table/test_pcg"
        datastack_info = {
            "datastack": "test_datastack",
            "aligned_volume": {"name": "test_aligned_volume"},
            "segmentation_source": "graphene://https://fake-daf.com/segmentation/table/test_pcg",
        }
        analysis_version = 1
        materialization_time_stamp = datetime.datetime.utcnow()
        mat_info = get_materialization_info(
            datastack_info, analysis_version, materialization_time_stamp
        )
        

    def test_collect_data(self):
        task = collect_data.s("test", {"some": "dict"}).apply()
        assert task.get() == (("test", {"some": "dict"}), {})

    def test_query_id_range(self):
        id_range = query_id_range(AnalysisVersion.id, 1, 3)
        assert (
            str(id_range)
            == "analysisversion.id >= :id_1 AND analysisversion.id < :id_2"
        )

    def test_chunk_ids(self, mat_metadata):
        table_name = mat_metadata["annotation_table_name"]
        schema = mat_metadata["schema_type"]
        model = make_annotation_model(table_name, schema, with_crud_columns=False)

        ids = chunk_ids(mat_metadata, model.id, 2)
        assert list(ids) == [[1, 3], [3, None]]

    def test_update_metadata(self, mat_metadata):
        is_updated = update_metadata.si(mat_metadata).apply()
        mat_ts = mat_metadata["materialization_time_stamp"]
        assert is_updated.get() == {
            "Table: test_synapse_table__test_pcg": f"Time stamp {mat_ts}"
        }

    def test_add_index(self, mat_metadata, db_client):
        database_name = mat_metadata["aligned_volume"]
        table_name = mat_metadata["annotation_table_name"]
        schema = mat_metadata["schema_type"]

        __, engine = db_client

        is_dropped = index_client.drop_table_indices(table_name, engine)
        assert is_dropped is True

        model = make_annotation_model(table_name, schema, with_crud_columns=False)

        indexes = index_client.add_indices_sql_commands(table_name, model, engine)
        for index in indexes:
            index = add_index.s(database_name, index).apply()
            assert "Index" or "Alter" in index.get()
