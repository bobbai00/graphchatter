import networkx as nx

# empty means no node or edge
def isEmpty(dag: nx.DiGraph) -> bool:
    return dag.number_of_nodes() == 0 and dag.number_of_edges() == 0

# single dot means has exactly one node and no edge
def isSingleDot(dag: nx.DiGraph) -> bool:
    return dag.number_of_nodes() == 1 and dag.number_of_edges() == 0

# strict chain is a DAG that has at least two nodes, and there is exactly one predecessor and one successor for each node, except for the source (no predecessor) and the sink (no successor)
def isSingleChain(dag: nx.DiGraph) -> bool:
    # Check if the graph is a DAG
    if not nx.is_directed_acyclic_graph(dag):
        return False

    # Ensure the graph has exactly one connected component
    if len(list(nx.weakly_connected_components(dag))) != 1:
        return False

    # Ensure the graph has exactly one source node
    sources = [node for node in dag.nodes if dag.in_degree(node) == 0]
    if len(sources) != 1:
        return False

    # Check if every node has exactly one predecessor and one successor
    for node in dag.nodes:
        in_deg = dag.in_degree(node)
        out_deg = dag.out_degree(node)

        # Source node should have one successor and no predecessor
        # Sink node should have one predecessor and no successor
        # All other nodes should have exactly one predecessor and one successor
        if not (in_deg == 1 and out_deg == 1) and not (
                (in_deg == 0 and out_deg == 1) or (in_deg == 1 and out_deg == 0)
        ):
            return False

    return True

def isSingleTree(dag: nx.DiGraph) -> bool:
    # Check if the graph is a DAG
    if not nx.is_directed_acyclic_graph(dag):
        return False

    # Ensure the graph has more than one node (exclude "dot" cases)
    if dag.number_of_nodes() <= 1:
        return False

    # Ensure the graph has exactly one connected component
    if len(list(nx.weakly_connected_components(dag))) != 1:
        return False

    # Ensure the graph has exactly one source node
    sources = [node for node in dag.nodes if dag.in_degree(node) == 0]
    if len(sources) != 1:
        return False

    # Ensure the graph is not a strict chain (not every node should have exactly one predecessor and one successor)
    is_chain = True
    for node in dag.nodes:
        in_deg = dag.in_degree(node)
        out_deg = dag.out_degree(node)
        if not ((in_deg == 1 and out_deg == 1) or
                (in_deg == 0 and out_deg == 1) or
                (in_deg == 1 and out_deg == 0)):
            is_chain = False
            break

    if is_chain:
        return False

    # In a tree, every node except the root should have exactly one predecessor.
    for node in dag.nodes:
        if dag.in_degree(node) > 1:
            return False

    return True

# single DAG means the graph is a tree, but nodes may have multiple predecessors and/or successors
def isSingleDAG(dag: nx.DiGraph) -> bool:
    # Check if the graph is a DAG
    if not nx.is_directed_acyclic_graph(dag):
        return False

    # Ensure the graph has exactly one connected component
    if len(list(nx.weakly_connected_components(dag))) != 1:
        return False

    # Ensure that at least one node has multiple predecessors
    has_multiple_predecessors = any(dag.in_degree(node) > 1 for node in dag.nodes)
    if not has_multiple_predecessors:
        return False

    return True