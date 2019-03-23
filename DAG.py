import sys
sys.path.insert(0, 'dag_generator')

from graph import Graph


class DAG(Graph):

    def __init__(self, GraphConfig):
        Graph.__init__(self, GraphConfig)
        self.linkCost = list([0] * len(self.treelinks))
        self.nodeCost = [[0] * GraphConfig.processor_count for i in range(len(self.nodes))]
