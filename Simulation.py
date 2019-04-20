import os
import random
import copy

from GraphGenerator import GraphGenerator
from graph import Graph, GraphConfig
from DAG import DAG
from MHEFT import FMHEFT, WPMHEFT, PPMHEFT

# Generate
DEADLINE = 50  # this can be change dynamically after loading
PROCESSOR_NUMBER = 48  # this can be change dynamically after loading, max num of processor

TASK_NUMBER_MIN = 10
TASK_NUMBER_MAX = 50
TASK_NUMBER_STEP = 5
DEPTH_MIN = 3
DEPTH_MAX = 20
OUTDEGREE_MIN = 1
OUTDEGREE_MAX = 5

# ratio of comm cost to task computation
COMM_COST_RATIO = [20]

TASK_COST_MIN = 10
TASK_COST_MAX = 100

GENERATE_TIMES_PER_COMBINATION = 5

# Analyze
PROCESSOR_NO_USED = [2, 4, 8, 16, 32, 48]
DEADLINE_RANGE = [5, 10, 20, 30, 40, 50]
APPLICATION_NO_USED = [5, 10, 30, 40, 50, 60, 70]
ANALYZE_TIMES_PER_COMBINATION = 10

OUTPUT_FOLDER = 'output_paper'

def check_task_depth_outdegree_is_valid(tasks, depth, outdegree):
    validity = True

    if depth <= 2:
        validity = False

    list_number = (tasks - 1) / outdegree
    list_per_level = (list_number - 1) / (depth - 2)

    if list_per_level < 1:
        validity = False

    return validity


def get_fat_index(treelevels):
    depth = 0
    width = 0
    for level in treelevels[1:-1]:
        depth += 1
        for layer in level:
            if len(layer) > width:
                width = len(layer)
    fat_index = float(width) / float(depth)
    multiplier = 0.5

    return round(int(fat_index / multiplier) * multiplier, 1)


def generate_data():
    print('Start generate simulation')

    load_graph = None
    store_graph = False
    outdir = "."

    gg = GraphGenerator()
    count = 0

    for task_size in range(TASK_NUMBER_MIN, TASK_NUMBER_MAX + TASK_NUMBER_STEP, TASK_NUMBER_STEP):
        for depth in range(DEPTH_MIN, DEPTH_MAX + 1):
            for outdegree in range(OUTDEGREE_MIN, OUTDEGREE_MAX + 1):
                valid_tdo = check_task_depth_outdegree_is_valid(task_size, depth, outdegree)

                if valid_tdo:
                    for comm_cost_ratio in COMM_COST_RATIO:
                        comm_min = int(TASK_COST_MIN * comm_cost_ratio / 100)
                        comm_max = int(TASK_COST_MAX * comm_cost_ratio / 100)

                        for times in range(GENERATE_TIMES_PER_COMBINATION):
                            print("generating task size of {}, depth of {}, outdegree of {}, "
                                  "comm ratio of {}. {} times".
                                  format(task_size, depth, outdegree, comm_cost_ratio, times + 1))

                            graph_arg = \
                                GraphGenerator.GraphArgs(add=None, dag='none', dead_line=DEADLINE, delete=None, depth=depth,
                                                         dot=False, load_graph=load_graph,
                                                         max_link_cost=comm_max, max_node_cost=TASK_COST_MAX,
                                                         min_link_cost=comm_min, min_node_cost=TASK_COST_MIN,
                                                         outdegree=outdegree, output_directory=outdir,
                                                         processor=PROCESSOR_NUMBER, redundancy=None, relabel=None,
                                                         reorder=None, size=task_size, spine=None, store_graph=store_graph,
                                                         summary=False, swap_links=None, swap_nodes=None, upper=False)
                            gg.set_arguments(graph_arg)
                            count += 1

                            # generate working directory
                            comm_cost_dir = os.getcwd() + "/" + OUTPUT_FOLDER + "/comm_ratio_" + str(comm_cost_ratio)
                            if not os.path.exists(os.path.join(comm_cost_dir)):
                                os.mkdir(comm_cost_dir)

                            fat_index = get_fat_index(gg.get_dag().treelevels)
                            if fat_index >= 0.0:
                                fat_dir = comm_cost_dir + "/FAT_" + str(fat_index)

                                if not os.path.exists(fat_dir):
                                    os.mkdir(fat_dir)

                                gg.store_dag(fat_dir)

    print('total files', count)


def analyze_data():
    output_root = os.getcwd() + "/" + OUTPUT_FOLDER
    populate_random = False

    # COMM RATIO
    comm_directory = ["comm_ratio_" + str(ratio) for ratio in COMM_COST_RATIO]

    report_path = os.getcwd() + "/report_paper_fat4.0.txt"
    f = open(report_path, "w")
    if not f:
        print('unable to write to file', report_path)
        return

    for comm_dir in comm_directory:
        comm_full_dir = output_root + '/' + comm_dir

        # FAT RATIO
        fat_directories = os.listdir(comm_full_dir)
        fat_directories = ["fat_4.0"]
        for fat_dir in fat_directories:
            fat_full_path = comm_full_dir + '/' + fat_dir
            # APPLICATION NO IN USE
            fat_files = [txt_file for txt_file in os.listdir(fat_full_path) if txt_file.endswith(".txt")]
            for application_count in APPLICATION_NO_USED:
                for analyze_times in range(ANALYZE_TIMES_PER_COMBINATION):
                    if len(fat_files) >= application_count:
                        application_files = random.sample(fat_files, application_count)

                        dag_list = list()
                        priority = 0

                        # load dag application
                        for application in application_files:
                            dag_file = fat_full_path + '/' + application
                            graph_config = GraphConfig(populate_random, not populate_random,
                                                       0, 0, 0, 'none', False,
                                                       dag_file, dag_file,
                                                       0, 0, 0, 0, 0, 0)
                            dag = DAG(graph_config)
                            priority += 1
                            dag_list.append(dag)

                        for processor_cnt in PROCESSOR_NO_USED:
                            for deadline_ratio in DEADLINE_RANGE:
                                result = "comm ratio: " + comm_dir + \
                                         " ; FAT ratio: " + fat_dir + \
                                         " ; applications no: " + str(application_count) + \
                                         " ; processor no: " + str(processor_cnt) + \
                                         " ; deadline ratio : " + str(deadline_ratio) + '\n'
                                print("COMM ratio : {} , FAT ratio : {} , applications : {} , "
                                      "processor no : {} , deadline : {}"
                                      .format(comm_dir, fat_dir, application_count, processor_cnt, deadline_ratio))

                                fmheft = FMHEFT(processor_cnt)
                                wpmheft = WPMHEFT(processor_cnt)
                                ppmheft = PPMHEFT(processor_cnt)
                                deadline_list = list()
                                deadval_list = list()
                                for app in dag_list:
                                    # PROCESSOR
                                    app_ref = app
                                    app_ref.set_processor_count(processor_cnt)

                                    # DEADLINE
                                    deadline = app_ref.lowerbound + int(app_ref.lowerbound / deadline_ratio)
                                    app_ref.set_deadline(deadline)

                                    # app priority is by EDF
                                    insert_index = 0
                                    for d in range(len(deadline_list)):
                                        if deadline <= deadline_list[d].deadline:
                                            break
                                        insert_index = d + 1
                                    deadline_list.insert(insert_index, app_ref)
                                    deadval_list.insert(insert_index, deadline)

                                for app_index in range(len(deadline_list)):
                                    app_ref = deadline_list[app_index]
                                    app_ref.set_application_priority(app_index + 1)

                                    fmheft.add_applications(copy.deepcopy(app_ref))
                                    wpmheft.add_applications(copy.deepcopy(app_ref))
                                    ppmheft.add_applications(copy.deepcopy(app_ref))

                                fmheft.find_makespan()
                                wpmheft.find_makespan()
                                ppmheft.find_makespan()

                                result += fmheft.get_result()
                                result += wpmheft.get_result()
                                result += ppmheft.get_result()

                                f.write(result)
                                f.write('\n')

    f.close()


if __name__ == '__main__':
    # generate_data()

    analyze_data()
