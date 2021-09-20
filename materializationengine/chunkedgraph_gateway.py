try:
    from pychunkedgraph.graph import chunkedgraph
except:
    from pychunkedgraph.backend import chunkedgraph

# TODO: put this in the infoservice

v1_chunkedgraph_version_mapping = ["fly_v26", "fly_v31"]


def check_pcg_mapping(table_id):
    v1_pcg_set = set(v1_chunkedgraph_version_mapping)
    table_id_set = set([table_id])
    return bool((v1_pcg_set & table_id_set))


class ChunkedGraphGateway:
    def __init__(self):
        self._cg = {}

    def init_pcg(self, table_id: str):
        self.is_pcg_v1 = check_pcg_mapping(table_id)

        if table_id not in self._cg:
            if self.is_pcg_v1:
                self._cg[table_id] = chunkedgraph.ChunkedGraph(table_id=table_id)
            else:
                self._cg[table_id] = chunkedgraph.ChunkedGraph(graph_id=table_id)

        return self._cg[table_id]


chunkedgraph_cache = ChunkedGraphGateway()
