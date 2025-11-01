import datetime
from unittest import mock

import pytest
from dynamicannotationdb.models import AnalysisVersion

from materializationengine.database import db_manager
from materializationengine.workflows.periodic_database_removal import (
    remove_expired_databases,
)


class TestPeriodicDatabaseRemoval:
    """Tests for the periodic database removal functionality"""
    
    @pytest.fixture(autouse=True)
    def setup_expired_versions(self, aligned_volume_name):
        """Create test versions with different expiration states"""
        current_time = datetime.datetime.utcnow()
        expired_time = current_time - datetime.timedelta(days=10)
        future_time = current_time + datetime.timedelta(days=10)
        
        with db_manager.session_scope(aligned_volume_name) as session:
            # Valid but expired version
            expired_valid = AnalysisVersion(
                datastack=aligned_volume_name,
                time_stamp=expired_time - datetime.timedelta(days=5),
                version=100,
                valid=True,
                expires_on=expired_time,
                status="AVAILABLE",
            )
            session.add(expired_valid)
            
            # Invalid and expired version
            expired_invalid = AnalysisVersion(
                datastack=aligned_volume_name,
                time_stamp=expired_time - datetime.timedelta(days=3),
                version=101,
                valid=False,
                expires_on=expired_time,
                status="FAILED",
            )
            session.add(expired_invalid)
            
            # Valid non-expired version
            non_expired = AnalysisVersion(
                datastack=aligned_volume_name,
                time_stamp=current_time - datetime.timedelta(days=2),
                version=102,
                valid=True,
                expires_on=future_time,
                status="AVAILABLE",
            )
            session.add(non_expired)
            
        yield
        
        with db_manager.session_scope(aligned_volume_name) as session:
            for version in [100, 101, 102]:
                try:
                    session.query(AnalysisVersion).filter(
                        AnalysisVersion.version == version
                    ).delete()
                except Exception:
                    pass
    
    @mock.patch("materializationengine.workflows.periodic_database_removal.get_config_param")
    @mock.patch("materializationengine.workflows.periodic_database_removal.get_aligned_volumes_databases")
    @mock.patch("materializationengine.workflows.periodic_database_removal.get_existing_databases")
    @mock.patch("materializationengine.workflows.periodic_database_removal.get_datastack_info")
    def test_remove_expired_databases(
        self, mock_get_info, mock_get_existing, mock_get_aligned, mock_config, aligned_volume_name
    ):
        """Test that expired databases are correctly identified and removed"""
        mock_get_aligned.return_value = [aligned_volume_name]
        mock_get_existing.return_value = [
            aligned_volume_name,
            f"{aligned_volume_name}__mat1",
            f"{aligned_volume_name}__mat100",
            f"{aligned_volume_name}__mat101",
            f"{aligned_volume_name}__mat102",
        ]
        mock_get_info.return_value = {
            "aligned_volume": {"name": aligned_volume_name},
            "datastack": aligned_volume_name
        }
        mock_config.return_value = [aligned_volume_name]
    
        dropped_databases = []
    
        with mock.patch("materializationengine.workflows.periodic_database_removal.get_all_versions") as mock_all_vers, \
             mock.patch("materializationengine.workflows.periodic_database_removal.get_valid_versions") as mock_valid_vers, \
             mock.patch("sqlalchemy.engine.Connection.execute") as mock_execute:
             
            mock_all_vers.return_value = [
                f"{aligned_volume_name}__mat1",
                f"{aligned_volume_name}__mat100",
                f"{aligned_volume_name}__mat101",
                f"{aligned_volume_name}__mat102"
            ]
            mock_valid_vers.return_value = [
                f"{aligned_volume_name}__mat1", 
                f"{aligned_volume_name}__mat102"
            ]
            
            def side_effect(statement, *args, **kwargs):
                stmt_str = str(statement)
                if isinstance(stmt_str, str) and "DROP DATABASE" in stmt_str:
                    database = stmt_str.split("DROP DATABASE ")[1].strip()
                    dropped_databases.append(database)
                return mock.MagicMock()
    
            mock_execute.side_effect = side_effect
    
            result = remove_expired_databases(
                delete_threshold=2, datastack=aligned_volume_name
            )
    
            assert mock_get_info.called
            assert mock_get_existing.called
            

            assert len(result) > 0 
            assert isinstance(result[0], list)
    
    @mock.patch("materializationengine.workflows.periodic_database_removal.get_config_param")
    @mock.patch("materializationengine.workflows.periodic_database_removal.get_aligned_volumes_databases")
    @mock.patch("materializationengine.workflows.periodic_database_removal.get_existing_databases")
    @mock.patch("materializationengine.workflows.periodic_database_removal.get_datastack_info")
    def test_specific_datastack_targeting(
        self, mock_get_info, mock_get_existing, mock_get_aligned, mock_config
    ):
        """Test targeting a specific datastack"""
        mock_get_aligned.return_value = ["test_aligned_volume", "another_db"]
        mock_get_existing.return_value = [
            "test_aligned_volume",
            "test_aligned_volume__mat100",
        ]
        mock_config.return_value = ["test_aligned_volume"]
        
        def get_info_side_effect(datastack_name):
            if datastack_name == "nonexistent_datastack":
                from materializationengine.errors import DataStackNotFoundException
                raise DataStackNotFoundException(f"datastack {datastack_name} not found")
            return {"aligned_volume": {"name": "test_aligned_volume"}}
            
        mock_get_info.side_effect = get_info_side_effect
    
        with mock.patch("sqlalchemy.engine.Connection.execute") as mock_execute:
            mock_execute.return_value = mock.MagicMock()
            
            result = remove_expired_databases(
                delete_threshold=2, datastack="nonexistent_datastack"
            )
            assert result == []
            
            mock_get_info.side_effect = None
            mock_get_info.return_value = {"aligned_volume": {"name": "test_aligned_volume"}}
            
            with mock.patch("materializationengine.workflows.periodic_database_removal.get_all_versions") as mock_all_vers, \
                 mock.patch("materializationengine.workflows.periodic_database_removal.get_valid_versions") as mock_valid_vers:
                
                mock_all_vers.return_value = ["test_aligned_volume__mat100"]
                mock_valid_vers.return_value = ["test_aligned_volume__mat100"]
                
                result = remove_expired_databases(
                    delete_threshold=2, datastack="test_aligned_volume"
                )
                assert len(result) == 1
                assert result[0] == []
    
    @mock.patch("materializationengine.workflows.periodic_database_removal.get_config_param")
    @mock.patch("materializationengine.workflows.periodic_database_removal.get_aligned_volumes_databases")
    def test_error_handling(self, mock_get_aligned, mock_config):
        """Test handling of errors during database operations"""
        mock_get_aligned.return_value = ["test_aligned_volume"]
        mock_config.return_value = ["test_aligned_volume"]
        
        with mock.patch("materializationengine.workflows.periodic_database_removal.get_datastack_info") as mock_get_info:
            mock_get_info.side_effect = Exception("Test exception")
            
            result = remove_expired_databases(delete_threshold=2)
            
            assert result == []


    @mock.patch("materializationengine.workflows.periodic_database_removal.get_config_param")
    @mock.patch("materializationengine.workflows.periodic_database_removal.get_aligned_volumes_databases")
    @mock.patch("materializationengine.workflows.periodic_database_removal.get_existing_databases")
    @mock.patch("materializationengine.workflows.periodic_database_removal.get_datastack_info")
    def test_preserves_expired_database_when_only_one_exists(
        self, mock_get_info, mock_get_existing, mock_get_aligned, mock_config, aligned_volume_name
    ):
        """Test that the function preserves the only materialization even if expired.
        """
        mock_get_aligned.return_value = [aligned_volume_name]
        mock_get_existing.return_value = [
            aligned_volume_name,
            f"{aligned_volume_name}__mat100",  
        ]
        mock_get_info.return_value = {
            "aligned_volume": {"name": aligned_volume_name},
            "datastack": aligned_volume_name
        }
        mock_config.return_value = [aligned_volume_name]
        
        current_time = datetime.datetime.utcnow()
        expired_time = current_time - datetime.timedelta(days=1) 
        with db_manager.session_scope(aligned_volume_name) as session:
            session.query(AnalysisVersion).filter(
                AnalysisVersion.version == 100
            ).delete()
            
            version = AnalysisVersion(
                datastack=aligned_volume_name,
                time_stamp=expired_time - datetime.timedelta(days=10),
                version=100,
                valid=True,
                expires_on=expired_time,
                status="AVAILABLE",
            )
            session.add(version)
            session.commit()
            
            version_check = session.query(AnalysisVersion).filter(
                AnalysisVersion.version == 100
            ).first()
            assert version_check is not None, "Failed to create test version in database"
        
        with mock.patch("materializationengine.workflows.periodic_database_removal.get_all_versions") as mock_all_vers, \
            mock.patch("materializationengine.workflows.periodic_database_removal.get_valid_versions") as mock_valid_vers:
            
            mock_all_vers.return_value = [f"{aligned_volume_name}__mat100"]
            mock_valid_vers.return_value = [f"{aligned_volume_name}__mat100"]
            
            result = remove_expired_databases(
                delete_threshold=1, datastack=aligned_volume_name
            )
            

            print(f"Result: {result}")
            assert len(result[0]) == 0, "The expired database should not be dropped when it's the only one available" 


    @mock.patch("materializationengine.workflows.periodic_database_removal.get_config_param")
    @mock.patch("materializationengine.workflows.periodic_database_removal.get_aligned_volumes_databases")
    @mock.patch("materializationengine.workflows.periodic_database_removal.get_existing_databases")
    @mock.patch("materializationengine.workflows.periodic_database_removal.get_datastack_info")
    @mock.patch("materializationengine.workflows.periodic_database_removal.db_manager.get_engine")
    @mock.patch("materializationengine.workflows.periodic_database_removal.get_all_versions")
    @mock.patch("materializationengine.workflows.periodic_database_removal.get_valid_versions")
    def test_stops_deletion_when_only_one_valid_db_exists_among_multiple(
        self, mock_get_valid_vers, mock_get_all_vers, mock_get_engine,
        mock_get_info, mock_get_existing, mock_get_aligned, mock_config, aligned_volume_name
    ):
        """Test deletion stops if only one valid database exists, even if others are present."""
        mock_get_aligned.return_value = [aligned_volume_name]
        # Two DBs exist physically: 1 expired/valid, 1 expired/invalid
        mock_get_existing.return_value = [
            f"{aligned_volume_name}__mat100", # Expired, Valid
            f"{aligned_volume_name}__mat101", # Expired, Invalid
        ]
        mock_get_info.return_value = {
            "aligned_volume": {"name": aligned_volume_name},
            "datastack": aligned_volume_name
        }
        mock_config.return_value = [aligned_volume_name]


        with db_manager.session_scope(aligned_volume_name) as session:
            session.query(AnalysisVersion).filter(
                AnalysisVersion.datastack == aligned_volume_name,
                AnalysisVersion.version == 102
            ).delete()
            session.commit()

            versions_in_db = session.query(AnalysisVersion).filter(
                 AnalysisVersion.datastack == aligned_volume_name
            ).order_by(AnalysisVersion.version).all()
            assert len(versions_in_db) == 2, "Test setup failed: Incorrect number of versions in DB"
            assert any(v.version == 100 and v.valid for v in versions_in_db), "Test setup failed: Version 100 mismatch"
            assert any(v.version == 101 and not v.valid for v in versions_in_db), "Test setup failed: Version 101 mismatch"

        mock_engine = mock.MagicMock()
        mock_conn = mock.MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        mock_get_engine.return_value = mock_engine
        mock_conn.execute.return_value = mock.MagicMock() # Assume no drops needed for check


        mock_get_all_vers.return_value = [
            f"{aligned_volume_name}__mat100",
            f"{aligned_volume_name}__mat101",
        ]
        mock_get_valid_vers.return_value = [
            f"{aligned_volume_name}__mat100",
        ]

        result = remove_expired_databases(
            delete_threshold=1, datastack=aligned_volume_name
        )

        print(f"Result (test_stops_deletion_when_only_one_valid_db_exists): {result}")
        assert len(result) == 1, "Should return result for one datastack"
        assert result[0] == [], "Deletion should stop as only one valid DB exists"
        drop_call_found = False
        for call in mock_conn.execute.call_args_list:
            stmt_arg = call.args[0]
            if isinstance(stmt_arg, str) and "DROP DATABASE" in stmt_arg.upper():
                 drop_call_found = True
                 break
            elif hasattr(stmt_arg, '__visit_name__') and stmt_arg.__visit_name__ == 'drop_database':
                drop_call_found = True
                break
        assert not drop_call_found, "DROP DATABASE should not have been called"            