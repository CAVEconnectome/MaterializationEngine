from dynamicannotationdb.models import AnalysisTable, AnalysisVersion


class TestModels:
    def test_analysis_version(self, mat_metadata):
        analysisversion = AnalysisVersion(
            datastack=mat_metadata["datastack"],
            time_stamp=mat_metadata["timestamp"],
            version=mat_metadata["version"],
            valid=False,
            expires_on=mat_metadata["expires_timestamp"],
        )
        assert analysisversion.datastack == mat_metadata["datastack"]
        assert analysisversion.time_stamp == mat_metadata["timestamp"]
        assert analysisversion.version == mat_metadata["version"]
        assert analysisversion.valid == False
        assert analysisversion.expires_on == mat_metadata["expires_timestamp"]

    def test_analysis_table(self, mat_metadata):
        analysis_table = AnalysisTable(
            aligned_volume=mat_metadata["aligned_volume"],
            schema=mat_metadata["schema"],
            table_name=mat_metadata["annotation_table_name"],
            valid=True,
            created=mat_metadata["timestamp"],
            analysisversion_id=mat_metadata["version"],
        )
        assert analysis_table.aligned_volume == mat_metadata["aligned_volume"]
        assert analysis_table.schema == mat_metadata["schema"]
        assert analysis_table.table_name == mat_metadata["annotation_table_name"]
        assert analysis_table.created == mat_metadata["timestamp"]
        assert analysis_table.analysisversion_id == mat_metadata["version"]
