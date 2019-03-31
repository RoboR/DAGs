import sys
sys.path.insert(0, 'dag_generator')
import math
import operator
from random import randint
from collections import namedtuple

from graph import Graph, GraphConfig
from DAG import DAG


TaskSlot = namedtuple('TaskSlot', ['application', 'task', 'startTime', 'endTime'])

def TraverseGraph(graphDict, node, targetNode, parent, path, includeNode=False):
    for child in graphDict[node]:
        TraverseGraph(graphDict, child, targetNode, parent + node, path)

    if targetNode is node:
        p = parent
        if includeNode:
            p = p + node
        if p:
            path.append(p)

def ReverseDict(d, root):
    result = {}
    result[root] = []

    for key in d:
        for val in d[key]:
            if result.get(val) is None:
                result[val] = []
            result.get(val).append(key)
    return result

def GetExitPath(readGraph, currentNode):
    # Find the path from exit node to current node
    reverseGraph = ReverseDict(readGraph.to_python_dict(), readGraph.find_root())
    reverseLevel = readGraph.treelevels[::-1]
    path = list()

    for i in reverseLevel[0]:
        for j in i:
            TraverseGraph(reverseGraph, j, currentNode, '', path)

    return path

def MaxiumumSuccesorCost(rankU, treeLevels, treelinks, paths, node):
    oriPos = FindPositionInLink(treeLevels, node)
    maxCost = 0

    for succ in paths:
        destNode = succ[-1]
        succRank = 0
        if rankU.get(destNode):
            succRank = rankU[destNode]
        else:
            print('cant get', destNode, rankU)
        destPos = FindPositionInLink(treeLevels, destNode)
        cCost = FindCommunicationCost(treelinks, oriPos, destPos)

        if (succRank + cCost) > maxCost:
            maxCost = succRank + cCost

    return maxCost

def FindPositionInLink(treelevel, node):
    for level in range(len(treelevel)):
        for block in range(len(treelevel[level])):
            for position in range(len(treelevel[level][block])):
                curPos = Graph.Position(level, block, position)
                if treelevel[level][block][position] is node:
                    return curPos

def FindCommunicationCost(treeLinks, oriPos, destPos):
    for links in treeLinks:
        if links.orig == oriPos and links.dest == destPos:
            return links.cost

def AverageWorkload(node):
    avgW = int(math.ceil(sum(readGraph.nodeCost[node]) / 3.0))
    return avgW

def GetAllRankU(readGraph):
    rankU = dict()
    for n in readGraph.nodes:
        rankU[n] = []

    reverseLevel = readGraph.treelevels[::-1]
    for lvl3 in reverseLevel:
        for lvl2 in lvl3:
            for lvl1 in lvl2:
                # Find max communication cost
                paths = GetExitPath(readGraph, lvl1)
                if paths:
                    maxSuccCost = MaxiumumSuccesorCost(rankU, readGraph.treelevels, readGraph.treelinks, paths, lvl1)
                    rankU[lvl1] = AverageWorkload(lvl1) + maxSuccCost
                else:
                    rankU[lvl1] = AverageWorkload(lvl1)

    return rankU

def GetParentExecutingProcessor(taskSlot, parent):
    proc = -1
    endTime = -1
    for pIdx in range(len(taskSlot)):
        for task in taskSlot[pIdx]:
            if task.task is parent:
                proc = pIdx
                endTime = task.endTime
                break

    return (proc, endTime)

def GetMinIndexAndValue(itemList):
    minIdx = -1
    minVal = -1

    for idx in range(len(itemList)):
        if minVal < 0 or itemList[idx] < minVal:
            minIdx = idx
            minVal = itemList[idx]

    return (minIdx, minVal)

def FindEarliestStartTimeOfTask(taskSlot, pCount, parentNodes, node, nodeCost, treeLinks, treeLevels):
    sTimeInEachProc = [0, 0, 0]

    # Find earliest completion time of parents
    parentEFT = [0, 0, 0]
    if parentNodes:
        # completion time of parent task in ith processor
        for parent in parentNodes:
            pProc, endTime = GetParentExecutingProcessor(taskSlot, parent)
            if pProc >= 0:
                for pIdx in range(pCount):
                    cc = 0
                    if pIdx is not pProc:
                        cc = FindCommunicationCost(treeLinks,
                                                   FindPositionInLink(treeLevels, parent),
                                                   FindPositionInLink(readGraph.treelevels, node))
                    parentEFT[pIdx] = max(parentEFT[pIdx], endTime + cc)

    # Find slot to insert task
    for pIdx in range(pCount):
        prevEndTime = 0
        for task in taskSlot[pIdx]:
            endTime = prevEndTime + nodeCost[pIdx]
            if endTime < task.startTime:
                prevEndTime = endTime
                break
            else:
                prevEndTime = task.endTime
        sTimeInEachProc[pIdx] = max(prevEndTime, parentEFT[pIdx])

    fTimeInEachProc = map(operator.add, sTimeInEachProc, list(nodeCost))
    selProc, eft = GetMinIndexAndValue(fTimeInEachProc)

    return (selProc, sTimeInEachProc[selProc], eft)

def FindLowerBound(rankList, parentGraph, readGraph):
    taskSlot = [[], [], []]
    lowerBound = 0

    for node, rank in rankList:
        parentNodes = parentGraph[node]
        sel, sTime, endTime = FindEarliestStartTimeOfTask(taskSlot, readGraph.processors,
                                                          parentNodes, node, readGraph.nodeCost[node],
                                                          readGraph.treelinks, readGraph.treelevels)
        taskSlot[sel].append(TaskSlot(readGraph.id, node, sTime, endTime))
        lowerBound = endTime

    return lowerBound

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


    graphRankU = GetAllRankU(readGraph)
    sortedRankU = sorted(graphRankU.items(), key=operator.itemgetter(1))
    sortedRankU.reverse()
    print('RANKU', sortedRankU)

    parentGraph = ReverseDict(readGraph.to_python_dict(), readGraph.find_root())
    lowerBound = FindLowerBound(sortedRankU, parentGraph, readGraph)
    print('LOWERBOUND', lowerBound)

    DEATH_LINE_MAX = 40
    deathLine = randint(0, DEATH_LINE_MAX) + lowerBound
    print('DEATHLINE', deathLine)