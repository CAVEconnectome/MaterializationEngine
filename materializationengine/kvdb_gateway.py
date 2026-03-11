import os

from kvdbclient.bigtable import BigTableConfig
from kvdbclient.bigtable.client import Client


class KVDBGateway:
    def __init__(self, project: str, instance: str):
        self._clients = {}
        self._config = BigTableConfig(PROJECT=project, INSTANCE=instance, ADMIN=False)

    def get_client(self, table_id: str) -> Client:
        if table_id not in self._clients:
            self._clients[table_id] = Client(table_id=table_id, config=self._config)
        return self._clients[table_id]


kvdb_cache = KVDBGateway(
    project=os.environ.get("BIGTABLE_PROJECT", ""),
    instance=os.environ.get("BIGTABLE_INSTANCE", "pychunkedgraph"),
)
