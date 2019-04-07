import sys
sys.path.insert(0, 'dag_generator')
from graph import Graph

import math
import operator
from collections import namedtuple
from random import randint


def reverse_dict(d, root):
    result = dict()
    result[root] = []

    for key in d:
        for val in d[key]:
            if result.get(val) is None:
                result[val] = []
            result.get(val).append(key)

    return result


def get_min_index_and_value(item_list):
    min_idx = -1
    min_val = -1

    for idx in range(len(item_list)):
        if min_val < 0 or item_list[idx] < min_val:
            min_idx = idx
            min_val = item_list[idx]

    return min_idx, min_val


def get_parent_executing_processor(task_slot, parent, app_id):
    processor = -1
    end_time = -1
    for pIdx in range(len(task_slot)):
        for task in task_slot[pIdx]:
            if task.application == app_id and task.task is parent:
                processor = pIdx
                end_time = task.endTime
                break

    return processor, end_time


class DAG(Graph):
    TaskSlot = namedtuple('TaskSlot', ['application', 'task', 'startTime', 'endTime'])

    def __init__(self, graph_config):
        Graph.__init__(self, graph_config)
        self.priority = 0
        self.rank_u = self.__calculate_rank_u()

        if graph_config.populate_randomly:
            self.set_lowerbound(self.__find_lower_bound())
            self.set_deadline(randint(0, graph_config.dead_line) + self.lowerbound)

    def __calculate_rank_u(self):
        rank_u = dict()
        for n in self.nodes:
            rank_u[n] = []

        reverse_level = self.treelevels[::-1]
        for lvl3 in reverse_level:
            for lvl2 in lvl3:
                for lvl1 in lvl2:
                    # Find max communication cost
                    paths = self.__get_exit_path(lvl1)
                    if paths:
                        max_succ_cost = self.__maximum_successor_cost(rank_u, paths, lvl1)
                        rank_u[lvl1] = self.__average_workload(lvl1) + max_succ_cost
                    else:
                        rank_u[lvl1] = self.__average_workload(lvl1)

        asc_rank_u = sorted(rank_u.items(), key=operator.itemgetter(1))
        asc_rank_u.reverse()
        return asc_rank_u

    def __get_exit_path(self, current_node):
        # Find the path from exit node to current node
        reverse_graph = reverse_dict(self.to_python_dict(), self.find_root())
        reverse_level = self.treelevels[::-1]
        path = list()

        for i in reverse_level[0]:
            for j in i:
                self.__traverse_graph(reverse_graph, j, current_node, '', path)

        return path

    def __traverse_graph(self, graph_dict, node, target_node, parent, path, include_node=False):
        for child in graph_dict[node]:
            self.__traverse_graph(graph_dict, child, target_node, parent + ',' + node, path)

        if target_node is node:
            p = parent
            if include_node:
                p = p + node
            if p:
                path.append(p)

    def __maximum_successor_cost(self, rank_u, paths, node):
        ori_pos = self.__find_position_in_link(node)
        max_cost = 0

        for successor in paths:
            dest_node = successor.split(',')[-1]
            succ_rank = 0
            if rank_u.get(dest_node):
                succ_rank = rank_u[dest_node]

            dest_pos = self.__find_position_in_link(dest_node)
            c_cost = self.__find_communication_cost(ori_pos, dest_pos)

            if (succ_rank + c_cost) > max_cost:
                max_cost = succ_rank + c_cost
        return max_cost

    def __find_position_in_link(self, node):
        for level in range(len(self.treelevels)):
            for block in range(len(self.treelevels[level])):
                for position in range(len(self.treelevels[level][block])):
                    cur_pos = Graph.Position(level, block, position)
                    if self.treelevels[level][block][position] == node:
                        return cur_pos

    def __find_communication_cost(self, ori_pos, dest_pos):
        for links in self.treelinks:
            if links.orig == ori_pos and links.dest == dest_pos:
                return links.cost

    def __average_workload(self, node):
        avg_w = int(math.ceil(sum(self.nodeCost[node]) / 3.0))
        return avg_w

    def __find_lower_bound(self):
        reverse_graph = reverse_dict(self.to_python_dict(), self.find_root())
        task_slot = [[] for i in range(self.processors)]
        lower_bound = 0

        for node, rank in self.rank_u:
            parent_nodes = reverse_graph[node]
            processor, start_time, end_time = self.__find_earliest_start_time_of_task(task_slot, parent_nodes, node)
            self.insert_to_task_slot_list_ascending(task_slot, processor, node, start_time, end_time)
            lower_bound = end_time

        return lower_bound

    def __find_earliest_start_time_of_task(self, task_slot, parent_nodes, node):
        start_time = [0] * self.processors
        # Find earliest completion time of parents
        parent_eft = [0] * self.processors

        if parent_nodes:
            # completion time of parent task in ith processor
            for parent in parent_nodes:
                pproc, end_time = get_parent_executing_processor(task_slot, parent, self.id)
                if pproc >= 0:
                    for pIdx in range(self.processors):
                        cc = 0
                        if pIdx is not pproc:
                            cc = self.__find_communication_cost(self.__find_position_in_link(parent),
                                                                self.__find_position_in_link(node))
                        parent_eft[pIdx] = max(parent_eft[pIdx], end_time + cc)

        # Find slot to insert task
        for pIdx in range(self.processors):
            prev_end_time = parent_eft[pIdx]
            for task in task_slot[pIdx]:
                end_time = prev_end_time
                if (end_time + self.nodeCost[node][pIdx]) < task.startTime and \
                        (end_time + self.nodeCost[node][pIdx]) < task.endTime:
                    prev_end_time = end_time
                    break
                else:
                    prev_end_time = task.endTime
            start_time[pIdx] = max(prev_end_time, parent_eft[pIdx])

        finish_time = map(operator.add, start_time, list(self.nodeCost[node]))
        processor_sel, eft = get_min_index_and_value(finish_time)
        # print('parent node', self.id, node, parent_nodes, parent_eft, start_time, finish_time)

        return processor_sel, start_time[processor_sel], eft

    def insert_tasks_to_list(self, tasks_slot_list, node):
        reverse_graph = reverse_dict(self.to_python_dict(), self.find_root())
        parent_nodes = reverse_graph[node]

        processor, start_time, end_time = self.__find_earliest_start_time_of_task(tasks_slot_list, parent_nodes, node)
        self.insert_to_task_slot_list_ascending(tasks_slot_list, processor, node, start_time, end_time)

    def insert_to_task_slot_list_ascending(self, taskslot_list, processor, node, start_time, end_time):
        insert_index = -1
        for i in range(len(taskslot_list[processor])):
            t = taskslot_list[processor][i]
            if start_time < t.startTime and end_time < t.endTime:
                insert_index = i
                break

        if insert_index < 0:
            insert_index = len(taskslot_list[processor])
        taskslot_list[processor].insert(insert_index, self.TaskSlot(self.id, node, start_time, end_time))

    def set_application_priority(self, priority):
        self.priority = priority
