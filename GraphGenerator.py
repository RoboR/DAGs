import sys
sys.path.insert(0, 'dag_generator')
import argparse
from copy import deepcopy
from collections import namedtuple

from DAG import DAG
from graph import Graph, GraphConfig
from mutations import MutateGraph


def graph_arguments_to_graph_config(args):
    populate_random = not args.load_graph

    gc = GraphConfig(populate_random,
                     not populate_random,
                     args.size,
                     args.outdegree,
                     args.depth,
                     args.dag,
                     not args.upper,
                     args.output_directory,
                     args.output_directory,
                     args.processor,
                     args.min_node_cost,
                     args.max_node_cost,
                     args.min_link_cost,
                     args.max_link_cost,
                     args.dead_line)
    return gc


class GraphGenerator:
    GraphArgs = namedtuple('GraphArgs', ['add', 'dag', 'dead_line', 'delete', 'depth', 'dot', 'load_graph',
                                         'max_link_cost', 'max_node_cost', 'min_link_cost', 'min_node_cost',
                                         'outdegree', 'output_directory', 'processor', 'redundancy', 'relabel',
                                         'reorder', 'size', 'spine', 'store_graph', 'summary', 'swap_links',
                                         'swap_nodes', 'upper'])

    def __init__(self):
        d = "Generate random acyclic directed graphs and produce mutations to " +\
            "it. The tool acts as a little virtual machine to produce and " +\
            "modify directed acyclic graphs"
        parser = argparse.ArgumentParser(description=d)

        parser.add_argument("--size", dest="size",
                            type=int,
                            default=25,
                            help="Choose the size of the graph (default 25)")

        parser.add_argument("--outdegree", dest="outdegree",
                            type=int,
                            default=3,
                            help="Choose the outdegree of the graph (default 3)")

        parser.add_argument("--depth", dest="depth",
                            type=int,
                            default=3,
                            help="Choose the depth of the graph (default 3)")

        parser.add_argument("--upper", dest="upper", action="store_true",
                            help="Use upper case instead lower case")

        parser.add_argument("--dot", dest="dot",
                            action="store_true",
                            help="Generate a dot file of the generated graph")

        parser.add_argument("--dag", dest="dag",
                            type=str,
                            default="none",
                            help="Specify the density of the dag, if not " +
                                 "specified it will generate a tree",
                            choices=["none", "sparse", "medium", "dense"])

        parser.add_argument("--store-graph", dest="store_graph",
                            action="store_true",
                            help="Store the generated graph")

        parser.add_argument("--output-directory", dest="output_directory",
                            type=str,
                            default=".",
                            help="Specify the directory for the generated files")

        parser.add_argument("--load-graph", dest="load_graph",
                            type=str,
                            help="Load the graph from a file")

        parser.add_argument("--swap-nodes", dest="swap_nodes",
                            type=int,
                            help="Mutation that swaps two nodes. (Repeated " +
                                 "SWAP_NODES times)")

        parser.add_argument("--swap-links", dest="swap_links",
                            type=int,
                            help="Mutation that swaps a father and a child." +
                                 " (Repeated SWAP_LINKS times)")

        parser.add_argument("--add", dest="add",
                            type=int,
                            help="Mutation that adds a node. (Repeated ADD times)")

        parser.add_argument("--relabel", dest="relabel",
                            type=int,
                            help="Mutation that relabels one node with a label " +
                                 "from outside the domain. (Repeated RELABEL " +
                                 "times)")

        parser.add_argument("--spine", dest="spine",
                            type=int,
                            help="Mutation that reorders a the nodes in a path " +
                                 "from the root to the leafs. (Repeated REORDER " +
                                 "times)")

        parser.add_argument("--reorder", dest="reorder",
                            type=int,
                            help="Mutation that reorders the descecndants of a " +
                                 "given node. (Repeated REORDER times)")

        parser.add_argument("--redundancy", dest="redundancy",
                            type=int,
                            help="Mutation that adds redudancy to the graph by " +
                                 "duplicating nodes. (Repeated REDUNDANCY times)")

        parser.add_argument("--delete", dest="delete",
                            type=int,
                            help="Mutation that deletes a branch. (Repeated " +
                                 "DELETE times)")

        parser.add_argument("--summary", dest="summary", action="store_true",
                            help="Print a summary of the mutations")

        parser.add_argument("--processor", dest="processor",
                            type=int,
                            default=3,
                            help="The number of processors used, affects the node cost")

        parser.add_argument("--min-node-cost", dest="min_node_cost",
                            type=int,
                            default=1,
                            help="the minimum amount of node cost, different for each processor")

        parser.add_argument("--max-node-cost", dest="max_node_cost",
                            type=int,
                            default=100,
                            help="the maximum amount of node cost, different for each processor")

        parser.add_argument("--min-link-cost", dest="min_link_cost",
                            type=int,
                            default=1,
                            help="the minimum amount of link cost")

        parser.add_argument("--max-link-cost", dest="max_link_cost",
                            type=int,
                            default=50,
                            help="the maximum amount of link cost")

        parser.add_argument("--dead-line", dest="dead_line",
                            type=int,
                            default=50,
                            help="the maximum deadline period starting from lowerbound")

        args = parser.parse_args()
        graph_config = graph_arguments_to_graph_config(args)

        # Generate the first graph
        self.dag = DAG(graph_config)
        self.args = args

    def start_generate(self):
        args = self.args
        mutate_graph = False
        if args.swap_nodes or args.add or args.relabel or args.spine or\
           args.reorder or args.redundancy or args.delete or args.swap_links:
            mutate_graph = True

        # Create a copy of the graph to mutate
        if mutate_graph:
            g2 = deepcopy(self.dag)
            m = MutateGraph(g2)

        # Do the mutations
        if args.swap_nodes:
            m.swap_nodes(args.swap_nodes)

        if args.swap_links:
            m.swap_links(args.swap_links)

        if args.add:
            m.add_node(args.add)

        if args.relabel:
            m.relabel_node(args.relabel)

        if args.spine:
            m.reorder_path(args.spine)

        if args.reorder:
            m.reorder_block(args.reorder)

        if args.redundancy:
            m.redundancy(args.redundancy)

        if args.delete:
            m.delete_path(args.delete)

        if args.dot:
            self.dag.generate_dot()
            if mutate_graph:
                g2.generate_dot()

        if args.store_graph:
            self.dag.store_python_representation()
            self.dag.store_graph()
            if mutate_graph:
                g2.store_python_representation()

        if args.summary and mutate_graph:
            m.print_mutations_summary()
            m.store_mutation_opcodes_to_file()
            m.store_mutations_summary_to_file()

    def get_dag(self):
        return self.dag

    def set_arguments(self, graph_args):
        graph_config = graph_arguments_to_graph_config(graph_args)
        self.args = graph_args
        self.dag = DAG(graph_config)

    def store_dag(self, out_dir):
        self.dag.output_directory = out_dir
        self.dag.store_graph()
        self.dag.store_python_representation()

if __name__ == '__main__':
    gg = GraphGenerator()
    gg.start_generate()
