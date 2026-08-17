from caveclient.chunkedgraph import ChunkedGraphClient
from caveclient.auth import AuthClient
from caveclient.auth import default_global_server_address
import os


default_server_address = os.environ.get(
    "GLOBAL_SERVER_URL", default_global_server_address
)

# The read deployment, not the write one. Every call this gateway makes is a read
# (is_latest_roots, get_roots, get_past_ids, get_root_timestamps, get_delta_roots), and the two
# deployments run the same image with no read-only distinction. The write deployment autoscales
# from 1 replica and has no preStop hook, so when it scaled a terminating pod's listener closed
# while traffic was still routed to it, and materialize surfaced the resulting
# [Errno 111] Connection refused as a 500 to its own callers. The read deployment holds a much
# larger minimum, so the same churn is far less likely to leave no reachable pod.
DEFAULT_PCG_SERVICE = "http://pychunkedgraph-read-service/"


def _resolve_pcg_service():
    """Address of the chunkedgraph this instance should read from.

    In-cluster by default and overridable only through PCG_SERVER_URL. LOCAL_SERVER_URL is
    deliberately NOT consulted: it names the public local server (config.cfg sets it to
    https://<env>.<domain>, and the upload blueprint reads it from app config as a CORS origin),
    so resolving the chunkedgraph through it would send traffic out of the cluster and back in
    through the ingress instead of straight to the service via cluster DNS. The deployments that
    set it in the environment already set it to this same read-service address, so dropping the
    fallback changes nothing today -- it only removes the chance of a slow, surprising regression.
    """
    return os.environ.get("PCG_SERVER_URL") or DEFAULT_PCG_SERVICE


PCG_SERVICE = _resolve_pcg_service()


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
