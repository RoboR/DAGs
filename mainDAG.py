import sys
sys.path.insert(0, 'dag_generator')

from graph import Graph, GraphConfig
from DAG import DAG

if __name__ == '__main__':
    print('Start DAG')
    load_graph = 'output/graph-xT7i-representation.txt'
    outputDirectory = 'output'
    processorCount = 3
    minNodeCost = 10
    maxNodeCost = 100
    minLinkCost = 1
    maxLinkCost = 50

    writeConfig = GraphConfig(True, False, 15, 3, 5,
                             'medium', True, None,
                             outputDirectory,
                             processorCount, minNodeCost, maxNodeCost,
                             minLinkCost, maxLinkCost)

    loadConfig = GraphConfig(False, True, None, None, None,
                             None, False, load_graph,
                             outputDirectory,
                             processorCount, minNodeCost, maxNodeCost,
                             minLinkCost, maxLinkCost)

    # writeGraph = DAG(writeConfig)
    readGraph = DAG(loadConfig)

    print('ID: ', readGraph.id)
    print('NODES', readGraph.nodes)
    print('NODE COSTS', readGraph.nodeCost)
    print('LEVELS', readGraph.treelevels)
    print('LINKS', readGraph.treelinks)
