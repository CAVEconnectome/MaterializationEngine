import datetime
import logging
import time
from unittest import mock

from materializationengine.workflows.bulk_upload import get_gcs_file_info


class TestBulkUpload:
    @mock.patch("materializationengine.workflows.bulk_upload.gcsfs", autospec=True)
    def test_get_gcs_file_info(self, mock_gcsfs):
        mock_gcsfs.return_value.GCSFileSystem.return_value = "/test_data"
        mock_gcsfs.return_value.ls.return_value = "/test_data/bulk_upload_metadata.json"

        upload_timestamp = datetime.datetime.utcnow()

        bulk_upload_params = {
            "project": "test_data",
            "file_path": "bulk_upload_data",
            "column_mapping": {"test": "test", "col2": "test2"},
            "annotation_table_name": "test_upload_table",
            "segmentation_source": "test_pcg",
            "materialized_ts": time.time(),  # epoch time
        }
        gcs_info = get_gcs_file_info(upload_timestamp, bulk_upload_params)
        logging.info(gcs_info)
