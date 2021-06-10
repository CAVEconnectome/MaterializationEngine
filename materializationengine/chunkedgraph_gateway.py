try:
    from pychunkedgraph.graph import chunkedgraph
except:
    from pychunkedgraph.backend import chunkedgraph

# TODO: put this in the infoservice

chunkedgraph_version_mapping = {"minnie3_v1": 2, "fly_v26": 1, "fly_v31": 1}


class ChunkedGraphGateway:
    def __init__(self):
        self._cg = {}

    def init_pcg(self, table_id: str):
        if not table_id in chunkedgraph_version_mapping:
            raise KeyError(
                f"Table ID '{table_id}' must be one of {chunkedgraph_version_mapping.keys()}"
            )
        if table_id not in self._cg:
            if chunkedgraph_version_mapping[table_id] == 1:
                self._cg[table_id] = chunkedgraph.ChunkedGraph(table_id=table_id)
            elif chunkedgraph_version_mapping[table_id] == 2:
                self._cg[table_id] = chunkedgraph.ChunkedGraph(graph_id=table_id)
        return self._cg[table_id]


chunkedgraph_cache = ChunkedGraphGateway()
