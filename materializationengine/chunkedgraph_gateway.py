import os

from caveclient.auth import AuthClient, default_global_server_address
from caveclient.chunkedgraph import ChunkedGraphClient

default_server_address = os.environ.get(
    "GLOBAL_SERVER_URL", default_global_server_address
)

PCG_SERVICE = os.environ.get("LOCAL_SERVER_URL", "http://pychunkedgraph-service/")


class ChunkedGraphGateway:
    def __init__(
        self,
        token_file=None,
        server_address=PCG_SERVICE,
        global_server_address=default_server_address,
    ):
        self._cg = {}
        self.server_address = server_address
        self.auth = AuthClient(
            token_file=token_file, server_address=global_server_address
        )

    def get_client(self, table_id: str):
        if table_id in self._cg.keys():
            return self._cg[table_id]
        else:
            return self.init_pcg(table_id)

    def init_pcg(self, table_id: str):

        cg_client = ChunkedGraphClient(
            self.server_address, table_name=table_id, auth_client=self.auth
        )
        self._cg[table_id] = cg_client
        return self._cg[table_id]


chunkedgraph_cache = ChunkedGraphGateway(
    token_file=os.environ.get("DAF_CREDENTIALS", None)
)
