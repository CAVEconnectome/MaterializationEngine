try:
    from pychunkedgraph.graph import chunkedgraph
except:
    from pychunkedgraph.backend import chunkedgraph

# TODO: put this in the infoservice


class ChunkedGraphGateway:
    def __init__(self):
        self._cg = {}

    def init_pcg(self, table_id: str):
        if table_id not in self._cg:
            try:
                self._cg[table_id] = chunkedgraph.ChunkedGraph(table_id=table_id)
            except TypeError:
                self._cg[table_id] = chunkedgraph.ChunkedGraph(graph_id=table_id)

        return self._cg[table_id]


chunkedgraph_cache = ChunkedGraphGateway()
