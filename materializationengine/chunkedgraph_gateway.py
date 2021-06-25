from caveclient.chunkedgraph import ChunkedGraphClient
from caveclient.auth import AuthClient
from caveclient.auth import default_global_server_address
import os

chunkedgraph_version_mapping = {"minnie3_v1": 2, "fly_v26": 1, "fly_v31": 1}

default_server_address = os.environ.get(
    "GLOBAL_SERVER_ADDRESS", default_global_server_address)

PCG_SERVICE = os.environ.get("PCG_SERVICE", "http://pychunkedgraph-service/")


class ChunkedGraphGateway:
    def __init__(self, token_file=None,
                 server_address=PCG_SERVICE,
                 global_server_address=default_server_address):
        self._cg = {}
        self.server_address = server_address
        self.auth = AuthClient(
            token_file=token_file, server_address=global_server_address)

    def init_pcg(self, table_id: str):

        cg_client = ChunkedGraphClient(
            self.server_address, table_name=table_id, auth_client=self.auth)
        self._cg[table_id] = cg_client
        return self._cg[table_id]


chunkedgraph_cache = ChunkedGraphGateway(
    token_file=os.environ.get("DAF_CREDENTIALS", None)
)
