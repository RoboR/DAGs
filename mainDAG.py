import sys
sys.path.insert(0, 'dag_generator')
import math
import operator
from random import randint
from collections import namedtuple
from Queue import Queue

from graph import Graph, GraphConfig
from DAG import DAG


class FMHEFT:
    CommonSlot = namedtuple('CommonSlot', ['app_priority', 'task'])

    def __init__(self, proccesor_count):
        self.applications = dict()
        self.applicationCount = 0

        self.task_priority_queue = Queue()
        self.common_ready_queue = Queue()
        self.task_allocation_queue = [[] for i in range(proccesor_count)]

    def add_applications(self, application):
        if self.applications.get(application.priority):
            print('same priority exists for %d' % application.priority)
            return

        self.applications[application.priority] = application
        self.applicationCount += 1

    def find_makespan(self):
        makespan_list = dict()

        # Put all tasks into task priority queue of applications according to descending order of rankU
        for app in self.applications:
            self.task_priority_queue.put((app, self.applications[app].rank_u))
            makespan_list[self.applications[app].id] = 0

        while not self.task_priority_queue.empty():
            max_tasks = self.__dequeue_task_priority_queue()
            for tasks in max_tasks:
                common_slot = FMHEFT.CommonSlot(tasks[0], tasks[1][0])
                self.common_ready_queue.put(common_slot)

            while not self.common_ready_queue.empty():
                slot = self.common_ready_queue.get()
                self.applications[slot.app_priority].insert_tasks_to_list(self.task_allocation_queue, slot.task)

        for processor_tasks in self.task_allocation_queue:
            for task in processor_tasks:
                if task.endTime > makespan_list[task.application]:
                    makespan_list[task.application] = task.endTime

        return makespan_list

    def __dequeue_task_priority_queue(self):
        temp_queue = Queue()
        max_tasks = list()

        while not self.task_priority_queue.empty():
            task_list = self.task_priority_queue.get()
            max_tasks.append((task_list[0], task_list[1][0]))

            del task_list[1][0]
            if task_list[1]:
                temp_queue.put(task_list)

        self.task_priority_queue = temp_queue
        return max_tasks

class WPMHEFT:
    CommonSlot = namedtuple('CommonSlot', ['app_priority', 'task'])

    def __init__(self, proccesor_count):
        self.applications = dict()
        self.applicationCount = 0

        self.task_priority_queue = Queue()
        self.application_priority_queue = Queue()
        self.task_allocation_queue = [[] for i in range(proccesor_count)]

    def add_applications(self, application):
        if self.applications.get(application.priority):
            print('same priority exists for %d' % application.priority)
            return

        self.applications[application.priority] = application
        self.applicationCount += 1

    def find_makespan(self):
        makespan_list = dict()

        # Put all tasks into task priority queue of applications according to descending order of rankU
        for app in self.applications:
            self.task_priority_queue.put(app)
            makespan_list[self.applications[app].id] = 0

        while not self.task_priority_queue.empty():
            priority = self.task_priority_queue.get()
            tasks_list = self.applications[priority].rank_u
            for task, rank_u in tasks_list:
                self.applications[priority].insert_tasks_to_list(self.task_allocation_queue, task)

        for processor_tasks in self.task_allocation_queue:
            for task in processor_tasks:
                if task.endTime > makespan_list[task.application]:
                    makespan_list[task.application] = task.endTime

        return makespan_list


if __name__ == '__main__':
    print('Start DAG')
    task_b_path = 'output/task-b.txt'
    task_b_config = GraphConfig(False, True, None, None, None,
                                None, False, task_b_path,
                                'output',
                                0, 0, 0, 0, 0, 0)
    DAG_b = DAG(task_b_config)
    task_a_path = 'output/task-a.txt'
    task_a_config = GraphConfig(False, True, None, None, None,
                                None, False, task_a_path,
                                'output',
                                0, 0, 0, 0, 0, 0)
    DAG_a = DAG(task_a_config)

    DAG_b.set_application_priority(1)
    DAG_a.set_application_priority(2)

    # # F_MHEFT
    # f_mheft = FMHEFT(3)
    # f_mheft.add_applications(DAG_a)
    # f_mheft.add_applications(DAG_b)
    # print(f_mheft.find_makespan())


    # WP_MHEFT
    wp_mheft = WPMHEFT(3)
    wp_mheft.add_applications(DAG_b)
    wp_mheft.add_applications(DAG_a)
    print(wp_mheft.find_makespan())
    # print(DAG_a.rank_u)
    # print(DAG_b.rank_u)

    # graph_path_01 = 'output/graph-9JWu-representation.txt'
    # graph_path_02 = 'output/graph-f7I7-representation.txt'
    # graph_path_03 = 'output/graph-xNM8-representation.txt'
    # outputDirectory = 'output'
    #
    # graph_config_01 = GraphConfig(False, True, None, None, None,
    #                               None, False, graph_path_01,
    #                               outputDirectory,
    #                               0, 0, 0, 0, 0, 0)
    # graph_config_02 = GraphConfig(False, True, None, None, None,
    #                               None, False, graph_path_02,
    #                               outputDirectory,
    #                               0, 0, 0, 0, 0, 0)
    # graph_config_03 = GraphConfig(False, True, None, None, None,
    #                               None, False, graph_path_03,
    #                               outputDirectory,
    #                               0, 0, 0, 0, 0, 0)
    #
    # DAG_01 = DAG(graph_config_01)
    # DAG_02 = DAG(graph_config_02)
    # DAG_03 = DAG(graph_config_03)
    # DAG_01.set_application_priority(1)
    # DAG_02.set_application_priority(2)
    # DAG_03.set_application_priority(3)
    # print(DAG_01.rank_u, DAG_01.lowerbound)

    # print('ID: ', DAG_01.id, DAG_02.id, DAG_03.id)
    # # print('NODES', DAG_01.nodes, DAG_02.nodes, DAG_03.nodes)
    # print('NODE COSTS', DAG_01.nodeCost, DAG_02.nodeCost, DAG_03.nodeCost)
    # # print('LEVELS', readGraph.treelevels)
    # # print('LINKS', readGraph.treelinks)
    # # print('DICT', readGraph.to_python_dict())
    # print('RANKU', DAG_01.rank_u, DAG_02.rank_u, DAG_03.rank_u)
    # # print('LOWERBOUND', readGraph.lower_bound)
    # print('DEATHLINE', DAG_01.deadline, DAG_02.deadline, DAG_03.deadline)
    # print('PRIORITY', DAG_01.priority, DAG_02.priority, DAG_03.priority)
    #
    # f_mheft = FMHEFT(3)
    # f_mheft.add_applications(DAG_01)
    # f_mheft.add_applications(DAG_03)
    # f_mheft.add_applications(DAG_02)
    #
    # # print(DAG_01.processors, DAG_02.processors, DAG_03.processors)
    # f_mheft.find_makespan()
