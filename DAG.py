import sys
sys.path.insert(0, 'dag_generator')

from graph import Graph


class DAG(Graph):

    def __init__(self, GraphConfig):
        Graph.__init__(self, GraphConfig)
