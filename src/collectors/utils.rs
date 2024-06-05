use dashmap::DashMap;
use petgraph::graph::{EdgeIndex, NodeIndex};

use crate::{
    config::CollectorConfig,
    git::CommitHash,
    graph::{CollectionExecutionGraph, CollectionGraphEdge, CollectionTask},
};

fn find_incoming_edges<F: Fn(&CollectionGraphEdge) -> bool>(
    graph: &CollectionExecutionGraph,
    current_node_idx: &NodeIndex,
    predicate: F,
) -> Vec<EdgeIndex> {
    let mut edge_walker = graph
        .graph
        .neighbors_directed(current_node_idx.clone(), petgraph::Direction::Incoming)
        .detach();

    let mut result = Vec::new();

    while let Some(edge_idx) = edge_walker.next_edge(&graph.graph) {
        let edge = &graph.graph[edge_idx];

        if predicate(edge) {
            result.push(edge_idx);
        }
    }

    result
}

pub fn find_preceding_node<
    EP: Fn(&CollectionGraphEdge) -> bool,
    NP: Fn(&CollectionTask) -> bool,
>(
    graph: &CollectionExecutionGraph,
    current_node_idx: &NodeIndex,
    edge_predicate: EP,
    node_predicate: NP,
) -> Option<NodeIndex> {
    let incoming_edges_matching_predicate =
        find_incoming_edges(graph, current_node_idx, edge_predicate);

    for edge_idx in incoming_edges_matching_predicate {
        let endpoints = graph.graph.edge_endpoints(edge_idx);

        if !endpoints.is_some() {
            continue;
        }

        let source_node_idx = endpoints.unwrap().0;
        let source_node = &graph.graph[source_node_idx];

        if node_predicate(source_node) {
            return Some(source_node_idx);
        }
    }

    None
}

pub fn get_previous_commit_value_of_collector(
    storage: &DashMap<(CollectorConfig, CommitHash), String>,
    graph: &CollectionExecutionGraph,
    current_node_idx: &NodeIndex,
) -> Option<String> {
    let current_node = &graph.graph[*current_node_idx];

    let previous_node_index = find_preceding_node(
        graph,
        current_node_idx,
        |e| e.distance == 1,
        |n| n.collector_config == current_node.collector_config,
    );

    if previous_node_index.is_none() {
        return None;
    }

    let previous_node_index = previous_node_index.unwrap();

    let previous_node = &graph.graph[previous_node_index];

    let value = storage.get(&(
        previous_node.collector_config.clone(),
        previous_node.commit_hash.clone(),
    ));

    match value {
        Some(value) => Some(value.clone()),
        None => None,
    }
}
