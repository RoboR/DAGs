import sys
sys.path.insert(0, 'dag_generator')
import math

from graph import Graph, GraphConfig
from DAG import DAG


def traverseGraph(graphDict, node, targetNode, parent, path, includeNode=False):
    for child in graphDict[node]:
        traverseGraph(graphDict, child, targetNode, parent + node, path)

    if targetNode is node:
        p = parent
        if includeNode:
            p = p + node
        if p:
            path.append(p)


def reverse_dict(d, root):
    result = {}
    result[root] = []

    for key in d:
        for val in d[key]:
            if result.get(val) is None:
                result[val] = []
            result.get(val).append(key)
    return result

def getExitPath(readGraph, currentNode):
    # Find the path from exit node to current node
    reverseGraph = reverse_dict(readGraph.to_python_dict(), readGraph.find_root())
    reverseLevel = readGraph.treelevels[::-1]
    path = list()

    for i in reverseLevel[0]:
        for j in i:
            traverseGraph(reverseGraph, j, currentNode, '', path)

    return path

def calculateRankU(readGraph):
    rankU = dict()
    for n in readGraph.nodes:
        rankU[n] = []

    reverseLevel = readGraph.treelevels[::-1]
    for lvl3 in reverseLevel:
        for lvl2 in lvl3:
            for lvl1 in lvl2:
                avgW = int(math.ceil(sum(readGraph.nodeCost[lvl1]) / 3.0))

                # Find max communication cost
                path = getExitPath(readGraph, lvl1)
                if path:
                    highestRankU = 0

                    for singlePath in path:
                        childRankUNode = singlePath[-1]
                        if rankU.get(childRankUNode):
                            for proc in range(readGraph.processors):
                                print(singlePath, type(singlePath), lvl1, childRankUNode, rankU[childRankUNode])
                else:
                    rankU[lvl1] = avgW
                    # print('not empty', path, lvl1)
                    # for list(singlePath) in path:
                    #     for node in

    print('rankU', rankU)

if __name__ == '__main__':
    print('Start DAG')
    load_graph = 'output/graph-DqnH-representation.txt'
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
    print('DICT', readGraph.to_python_dict())

    calculateRankU(readGraph)
