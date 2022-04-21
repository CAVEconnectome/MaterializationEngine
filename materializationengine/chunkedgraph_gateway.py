from caveclient.chunkedgraph import ChunkedGraphClient
from caveclient.auth import AuthClient
from caveclient.auth import default_global_server_address
import os

v1_chunkedgraph_version_mapping = ["fly_v26", "fly_v31"]


def check_pcg_mapping(table_id):
    v1_pcg_set = set(v1_chunkedgraph_version_mapping)
    table_id_set = set([table_id])
    return bool((v1_pcg_set & table_id_set))


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

    def init_pcg(self, table_id: str):

        cg_client = ChunkedGraphClient(
            self.server_address, table_name=table_id, auth_client=self.auth
        )
        self._cg[table_id] = cg_client
        return self._cg[table_id]


chunkedgraph_cache = ChunkedGraphGateway(
    token_file=os.environ.get("DAF_CREDENTIALS", None)
)
