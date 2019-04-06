import sys
sys.path.insert(0, 'dag_generator')
import math
import operator
from random import randint
from collections import namedtuple

from graph import Graph, GraphConfig
from DAG import DAG


if __name__ == '__main__':
    print('Start DAG')
    graph_path_01 = 'output/graph-Fsko-representation.txt'
    graph_path_02 = 'output/graph-r0WY-representation.txt'
    graph_path_03 = 'output/graph-Z2rG-representation.txt'
    outputDirectory = 'output'

    graph_config_01 = GraphConfig(False, True, None, None, None,
                                  None, False, graph_path_01,
                                  outputDirectory,
                                  0, 0, 0, 0, 0, 0)
    graph_config_02 = GraphConfig(False, True, None, None, None,
                                  None, False, graph_path_02,
                                  outputDirectory,
                                  0, 0, 0, 0, 0, 0)
    graph_config_03 = GraphConfig(False, True, None, None, None,
                                  None, False, graph_path_03,
                                  outputDirectory,
                                  0, 0, 0, 0, 0, 0)

    DAG_01 = DAG(graph_config_01)
    DAG_02 = DAG(graph_config_02)
    DAG_03 = DAG(graph_config_03)
    DAG_01.set_application_priority(1);
    DAG_02.set_application_priority(2);
    DAG_03.set_application_priority(3);

    print('ID: ', DAG_01.id, DAG_02.id, DAG_03.id)
    # print('NODES', DAG_01.nodes, DAG_02.nodes, DAG_03.nodes)
    # print('NODE COSTS', readGraph.nodeCost)
    # print('LEVELS', readGraph.treelevels)
    # print('LINKS', readGraph.treelinks)
    # print('DICT', readGraph.to_python_dict())
    print('RANKU', DAG_01.rank_u, DAG_02.rank_u, DAG_03.rank_u)
    # print('LOWERBOUND', readGraph.lower_bound)
    print('DEATHLINE', DAG_01.deadline, DAG_02.deadline, DAG_03.deadline)
    print('PRIORITY', DAG_01.priority, DAG_02.priority, DAG_03.priority)
