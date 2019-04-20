import sys
sys.path.insert(0, 'dag_generator')
import operator
from collections import namedtuple
from Queue import Queue
import copy

from graph import Graph, GraphConfig
from DAG import DAG


def check_tasks_is_in_queue(tasks_queue, application, targetTask):
    found = False
    for task_on_proc in tasks_queue:
        for app, task, startTime, endTime in task_on_proc:
            if app == application and task == targetTask:
                found = True

    return found


ScheduleResult = namedtuple('ScheduleResult', ['id', 'priority', 'lowerbound', 'deadline', 'makespan'])


class FMHEFT:
    CommonSlot = namedtuple('CommonSlot', ['app_priority', 'task', 'rank_u'])

    def __init__(self, proccesor_count):
        self.applications = dict()
        self.applicationCount = 0

        self.task_priority_queue = Queue()
        self.common_ready_queue = list()
        self.task_allocation_queue = [[] for i in range(proccesor_count)]
        self.makespan = None
        self.schedule_result = dict()
        self.task_allocate_by_ppmheft = False

    def add_applications(self, application):
        if self.applications.get(application.priority):
            print('same priority exists for %d' % application.priority)
            return

        self.applications[application.priority] = application
        self.applicationCount += 1

    def set_allocated_tasks(self, task_allocation):
        self.task_allocation_queue = task_allocation
        self.task_allocate_by_ppmheft = True

    def find_makespan(self):
        makespan_list = dict()

        # Put all tasks into task priority queue of applications according to descending order of rankU
        for app_priority in self.applications:
            self.task_priority_queue.put((app_priority,
                                          self.applications[app_priority].id,
                                          self.applications[app_priority].rank_u))
            makespan_list[self.applications[app_priority].id] = 0

        # remove the allocated task from rank_u list
        if self.task_allocate_by_ppmheft:
            task_allocated = list()
            for task_in_proc in self.task_allocation_queue:
                for task in task_in_proc:
                    if task:
                        task_allocated.append(task)

            priority_task_list = dict();
            while not self.task_priority_queue.empty():
                priority, app_id, tasks = self.task_priority_queue.get()
                priority_task_list[app_id] = (priority, dict(tasks))

            for task_slot in task_allocated:
                if priority_task_list.get(task_slot.application):
                    priority_task_list[task_slot.application][1].pop(task_slot.task)

            for app_id, (priority, rank_u_list) in priority_task_list.items():
                rank_u_items = sorted(rank_u_list.items(), key=operator.itemgetter(1), reverse=True)
                if rank_u_items:
                    self.task_priority_queue.put((priority, app_id, rank_u_items))

        while not self.task_priority_queue.empty():
            task_to_run_on_each_processor = self.__dequeue_task_priority_queue()
            for app_priority, (task, rank_u) in task_to_run_on_each_processor:
                self.common_ready_queue.append(FMHEFT.CommonSlot(app_priority, task, rank_u))
                self.common_ready_queue = sorted(self.common_ready_queue,
                                                 key=operator.itemgetter(2),
                                                 reverse=True)

            for app_priority, task, rank_u in self.common_ready_queue:
                # this is for PP_MHEFT to queue
                if not check_tasks_is_in_queue(self.task_allocation_queue,
                                               self.applications[app_priority].id,
                                               task):
                    self.applications[app_priority].insert_tasks_to_list(self.task_allocation_queue, task)
            self.common_ready_queue = list()

        for processor_tasks in self.task_allocation_queue:
            for task in processor_tasks:
                if not makespan_list.get(task.application) or task.endTime > makespan_list[task.application]:
                    makespan_list[task.application] = task.endTime

        self.makespan = makespan_list

        for app_prio in self.applications:
            app = self.applications[app_prio]
            self.schedule_result[app.id] = (ScheduleResult(app.id, app.priority, app.lowerbound,
                                                           app.deadline, makespan_list[app.id]))

        return makespan_list

    def __dequeue_task_priority_queue(self):
        temp_queue = Queue()
        task_to_run_on_each_processor = list()

        while not self.task_priority_queue.empty():
            app_priority, app_id, task_lists = self.task_priority_queue.get()
            task_to_run_on_each_processor.append((app_priority, task_lists[0]))

            del task_lists[0]

            if task_lists:
                temp_queue.put((app_priority, app_id, task_lists))

        self.task_priority_queue = temp_queue
        return task_to_run_on_each_processor


    def get_result(self):
        result = None
        if self.makespan:
            result = "F_MHEFT\n" + "Applications : " + str(len(self.applications)) + "\n"
            miss_deadline = 0
            overall_lateness = 0
            total_makespan = 0
            for app_prio in self.applications:
                app = self.applications[app_prio]
                sched_res = self.schedule_result[app.id]
                lateness = round(float(sched_res.makespan - sched_res.deadline) / float(sched_res.deadline), 4)
                overall_lateness += lateness
                if lateness > 0:
                    miss_deadline += 1
                if sched_res.makespan > total_makespan:
                    total_makespan = sched_res.makespan
                result += "app: " + str(app.id) + "; app priority: " + str(app_prio) \
                          + "; nodes: " + str(len(app.nodes)) \
                          + "; lowerbound: " + str(app.lowerbound) + "; deadline: " + str(app.deadline) \
                          + "; makespan: " + str(sched_res.makespan) + "; lateness: " + str(lateness) + "\n"
            deadline_miss_ratio = round(float(miss_deadline) / float(len(self.applications)), 2)
            result += "DMRs: " + str(deadline_miss_ratio) + "; overall lateness: " + str(overall_lateness) + \
                      "; total makespan: " + str(total_makespan) + "\n"

        return result


class WPMHEFT:
    def __init__(self, proccesor_count):
        self.applications = dict()
        self.applicationCount = 0

        self.task_priority_queue = list()
        self.application_priority_queue = Queue()
        self.task_allocation_queue = [[] for i in range(proccesor_count)]
        self.makespan = None
        self.schedule_result = dict()

    def add_applications(self, application):
        if self.applications.get(application.priority):
            print('same priority exists for %d' % application.priority)
            return

        self.applications[application.priority] = application
        self.applicationCount += 1

    def find_makespan(self):
        makespan_list = dict()

        # Put all tasks into task priority queue of applications according descending priority
        for app in self.applications:
            self.task_priority_queue.append(app)
            makespan_list[self.applications[app].id] = 0

        for app_priority in self.task_priority_queue:
            tasks_list = self.applications[app_priority].rank_u
            for task, rank_u in tasks_list:
                self.applications[app_priority].insert_tasks_to_list(self.task_allocation_queue, task)

        for processor_tasks in self.task_allocation_queue:
            for task in processor_tasks:
                if task.endTime > makespan_list[task.application]:
                    makespan_list[task.application] = task.endTime

        self.makespan = makespan_list
        for app_prio in self.applications:
            app = self.applications[app_prio]
            self.schedule_result[app.id] = (ScheduleResult(app.id, app.priority, app.lowerbound,
                                                           app.deadline, makespan_list[app.id]))

        return makespan_list

    def get_result(self):
        result = None
        if self.makespan:
            result = "WP_MHEFT\n" + "Applications : " + str(len(self.applications)) + "\n"
            miss_deadline = 0
            overall_lateness = 0
            total_makespan = 0
            for app_prio in self.applications:
                app = self.applications[app_prio]
                sched_res = self.schedule_result[app.id]
                lateness = round(float(sched_res.makespan - sched_res.deadline) / float(sched_res.deadline), 4)
                overall_lateness += lateness
                if lateness > 0:
                    miss_deadline += 1
                if sched_res.makespan > total_makespan:
                    total_makespan = sched_res.makespan
                result += "app: " + str(app.id) + "; app priority: " + str(app_prio) \
                          + "; nodes: " + str(len(app.nodes)) \
                          + "; lowerbound: " + str(app.lowerbound) + "; deadline: " + str(app.deadline) \
                          + "; makespan: " + str(sched_res.makespan) + "; lateness: " + str(lateness) + "\n"
            deadline_miss_ratio = round(float(miss_deadline) / float(len(self.applications)), 2)
            result += "DMRs: " + str(deadline_miss_ratio) + "; overall lateness: " + str(overall_lateness) + \
                      "; total makespan: " + str(total_makespan) + "\n"

        return result


class PPMHEFT:
    TaskSlot = namedtuple('TaskSlot', ['app_priority', 'task', 'rank_u'])

    def __init__(self, proccesor_count):
        self.applications = dict()
        self.applicationCount = 0
        self.processors = proccesor_count

        self.task_priority_queue = Queue()
        self.application_priority_queue = Queue()
        self.task_allocation_queue = [[] for i in range(proccesor_count)]
        self.makespan = None
        self.schedule_result = dict()

    def add_applications(self, application):
        if self.applications.get(application.priority):
            print('same priority exists for %d' % application.priority)
            return

        self.applications[application.priority] = application
        self.applicationCount += 1

    def find_makespan(self):
        makespan_list = dict()
        rank_u_list = list()
        remaining_app_priority = list()

        # put all applications into application_priority_queue according to descending order
        for app_priority in self.applications:
            self.application_priority_queue.put(app_priority)
            remaining_app_priority.append(app_priority)
            makespan_list[self.applications[app_priority].id] = 0

            # put all tasks into task_priority_queue according to descending order
            for task, rank_u in self.applications[app_priority].rank_u:
                slot = PPMHEFT.TaskSlot(-app_priority, task, rank_u)    # app_priority is negative is for sorting later
                rank_u_list.append(slot)

        sorted_rank_u = sorted(rank_u_list, key=operator.itemgetter(2, 0), reverse=True)
        for priority, task, rank_u in sorted_rank_u:
            self.task_priority_queue.put(PPMHEFT.TaskSlot(-priority, task, rank_u))

        while not self.application_priority_queue.empty():
            app_m_scheduled = False
            app_m_priority = self.application_priority_queue.get()
            low_application_set = [prio for prio in remaining_app_priority if prio >= app_m_priority]

            while not app_m_scheduled:
                # Probe fairly schedule app Gm and all the applications in low_application_set using F_MHEFT
                fmheft = FMHEFT(self.processors)
                fmheft.set_allocated_tasks(copy.deepcopy(self.task_allocation_queue))

                for app_priority in low_application_set:
                    fmheft.add_applications(copy.deepcopy(self.applications[app_priority]))

                app_m_fair_makespan = fmheft.find_makespan()

                # F_MHEFT makespan meets deadline
                app_m_makespan = app_m_fair_makespan[self.applications[app_m_priority].id]
                if app_m_makespan <= self.applications[app_m_priority].deadline:
                    app_m_scheduled = True
                    self.task_allocation_queue = fmheft.task_allocation_queue
                    remaining_app_priority.remove(app_m_priority)

                    # checks remaining app met deadline, cancel the remaining app allocation if it does not meet
                    all_scheduled = True
                    for remain_priority in remaining_app_priority:
                        remaining_app_makespan = app_m_fair_makespan[self.applications[remain_priority].id]
                        if remaining_app_makespan > self.applications[remain_priority].deadline:
                            all_scheduled = False
                    if not all_scheduled:
                        remaining_app_id = [self.applications[prio].id for prio in remaining_app_priority]

                        temp_task_allocation_queue = [[] for i in range(self.processors)]
                        for procIndex in range(self.processors):
                            for task in self.task_allocation_queue[procIndex]:
                                if task.application not in remaining_app_id:
                                    temp_task_allocation_queue[procIndex].append(task)
                        self.task_allocation_queue = temp_task_allocation_queue

                # cancel previous allocation of F_MHEFT
                else:
                    # get highest rank u task of app Gm
                    priority_q_temp = Queue()
                    task_m = None

                    while not self.task_priority_queue.empty():
                        # get first task of app Gm in task_priority_queue
                        task_slot = self.task_priority_queue.get()
                        if task_m is None and task_slot.app_priority == app_m_priority:
                            task_m = task_slot
                        else:
                            priority_q_temp.put(task_slot)

                    # all task for app_m has scheduled
                    if not task_m:
                        app_m_scheduled = True
                        remaining_app_priority.remove(app_m_priority)
                    self.task_priority_queue = priority_q_temp

                    # insert with insertion-based policy
                    if task_m:
                        app_m = copy.deepcopy(self.applications[task_m.app_priority])
                        app_m.insert_tasks_to_list(self.task_allocation_queue, task_m.task)

        for processor_tasks in self.task_allocation_queue:
            for task in processor_tasks:
                if task.endTime > makespan_list[task.application]:
                    makespan_list[task.application] = task.endTime

        self.makespan = makespan_list
        for app_prio in self.applications:
            app = self.applications[app_prio]
            self.schedule_result[app.id] = (ScheduleResult(app.id, app.priority, app.lowerbound,
                                                           app.deadline, makespan_list[app.id]))

        return makespan_list

    def get_result(self):
        result = None
        if self.makespan:
            result = "PP_MHEFT\n" + "Applications : " + str(len(self.applications)) + "\n"
            miss_deadline = 0
            overall_lateness = 0
            total_makespan = 0
            for app_prio in self.applications:
                app = self.applications[app_prio]
                sched_res = self.schedule_result[app.id]
                lateness = round(float(sched_res.makespan - sched_res.deadline) / float(sched_res.deadline), 4)
                overall_lateness += lateness
                if lateness > 0:
                    miss_deadline += 1
                if sched_res.makespan > total_makespan:
                    total_makespan = sched_res.makespan
                result += "app: " + str(app.id) + "; app priority: " + str(app_prio) \
                          + "; nodes: " + str(len(app.nodes)) \
                          + "; lowerbound: " + str(app.lowerbound) + "; deadline: " + str(app.deadline) \
                          + "; makespan: " + str(sched_res.makespan) + "; lateness: " + str(lateness) + "\n"
            deadline_miss_ratio = round(float(miss_deadline) / float(len(self.applications)), 2)
            result += "DMRs: " + str(deadline_miss_ratio) + "; overall lateness: " + str(overall_lateness) + \
                      "; total makespan: " + str(total_makespan) + "\n"

        return result


if __name__ == '__main__':
    print('Start DAG')

    task_1_path = 'output/task-1.dag'
    task_1_config = GraphConfig(False, True, None, None, None,
                                None, False, task_1_path,
                                'output',
                                0, 0, 0, 0, 0, 0)
    DAG_1 = DAG(task_1_config)

    task_2_path = 'output/task-2.dag'
    task_2_config = GraphConfig(False, True, None, None, None,
                                None, False, task_2_path,
                                'output',
                                0, 0, 0, 0, 0, 0)
    DAG_2 = DAG(task_2_config)

    task_3_path = 'output/task-3.dag'
    task_3_config = GraphConfig(False, True, None, None, None,
                                None, False, task_3_path,
                                'output',
                                0, 0, 0, 0, 0, 0)
    DAG_3 = DAG(task_3_config)

    DAG_1.set_application_priority(1)
    DAG_2.set_application_priority(2)
    DAG_3.set_application_priority(3)

    # F_MHEFT
    fmheft_paper = FMHEFT(3)
    fmheft_paper.add_applications(copy.deepcopy(DAG_1))
    fmheft_paper.add_applications(copy.deepcopy(DAG_2))
    fmheft_paper.add_applications(copy.deepcopy(DAG_3))
    makespan_fmheft = fmheft_paper.find_makespan()
    print(makespan_fmheft)
    print(fmheft_paper.task_allocation_queue)

    # WP_MHEFT
    wpmheft_paper = WPMHEFT(3)
    wpmheft_paper.add_applications(copy.deepcopy(DAG_1))
    wpmheft_paper.add_applications(copy.deepcopy(DAG_2))
    wpmheft_paper.add_applications(copy.deepcopy(DAG_3))
    makespan_wpmheft = wpmheft_paper.find_makespan()
    print(makespan_wpmheft)
    print(wpmheft_paper.task_allocation_queue)

    # PP_MHEFT
    ppmheft_paper = PPMHEFT(3)
    ppmheft_paper.add_applications(copy.deepcopy(DAG_1))
    ppmheft_paper.add_applications(copy.deepcopy(DAG_2))
    ppmheft_paper.add_applications(copy.deepcopy(DAG_3))
    makespan_ppmheft_paper = ppmheft_paper.find_makespan()
    print(makespan_ppmheft_paper)
    print(ppmheft_paper.task_allocation_queue)
