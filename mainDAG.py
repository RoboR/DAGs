import sys
sys.path.insert(0, 'dag_generator')

from graph import Graph, GraphConfig
from DAG import DAG

if __name__ == '__main__':
    print('Start DAG')
    load_graph = 'output/graph-SsiI-representation.txt'
    outputDirectory = 'output'
    processorCount = 2
    gc = GraphConfig(False, True, None, None, None,
                     None, False, load_graph,
                     outputDirectory,
                     processorCount)

    readGraph = DAG(gc)


    print((readGraph.linkCost))
    print(readGraph.nodes)
    print(readGraph.nodeCost)

