import datetime
import logging

from materializationengine.shared_tasks import get_materialization_info
from materializationengine.workflows.create_frozen_database import (
    add_indices,
    check_tables,
    create_analysis_database,
    create_materialized_metadata,
    create_new_version,
    drop_tables,
    merge_tables,
    update_table_metadata,
)

datastack_info = {
    "datastack": "test_aligned_volume",
    "aligned_volume": {"name": "test_aligned_volume"},
    "segmentation_source": "graphene://https://fake-daf.com/segmentation/table/test_pcg",
}

materialization_time_stamp = datetime.datetime.utcnow()


class TestCreateFrozenVersion:
    def test_create_new_version(self, test_app):

        new_version_number = create_new_version(
            datastack_info=datastack_info,
            materialization_time_stamp=materialization_time_stamp,
            days_to_expire=7,
        )
        assert new_version_number == 1

    def test_create_analysis_database(self):
        is_created = create_analysis_database.s(datastack_info, 1).apply()
        assert is_created.get() == True

    def test_create_materialized_metadata(self, mat_metadata):
        mat_info = get_materialization_info(
            datastack_info, 1, materialization_time_stamp
        )
        is_table_created = create_materialized_metadata.s(
            datastack_info=datastack_info,
            mat_info=mat_info,
            analysis_version=1,
            materialization_time_stamp=materialization_time_stamp,
        ).apply()
        assert is_table_created.get() == True

    def test_update_table_metadata(self, mat_metadata):
        tables = update_table_metadata.s([mat_metadata]).apply()
        assert tables.get() == ["test_synapse_table"]

    def test_drop_tables(self, mat_metadata):
        dropped_tables = drop_tables.s([mat_metadata], analysis_version=1).apply()
        logging.info(dropped_tables)
        assert dropped_tables.get() != None

    def test_merge_tables(self, mat_metadata):
        table_info = merge_tables.s(mat_metadata).apply()
        logging.info(table_info)
        assert table_info.get() == "Number of rows copied: 4"

    def test_add_indices(self, mat_metadata):
        index = add_indices.s(mat_metadata).apply()
        logging.info(index.get())
        assert "Index" in index.get()

    def test_check_tables(self, mat_metadata):
        table_info = check_tables.s([mat_metadata], 1).apply()
        assert (
            table_info.get()
            == "All materialized tables match valid row number from live tables"
        )
