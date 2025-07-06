use dashmap::DashMap;
use petgraph::graph::{EdgeIndex, NodeIndex};
use thiserror::Error;

use crate::{
    config::CollectorConfig,
    git::CommitHash,
    graph::{CollectionExecutionGraph, CollectionGraphEdge, CollectionTask},
};

use super::CollectorValue;

fn find_incoming_edges<F: Fn(&CollectionGraphEdge) -> bool>(
    graph: &CollectionExecutionGraph,
    current_node_idx: NodeIndex,
    predicate: F,
) -> Vec<EdgeIndex> {
    let mut edge_walker = graph
        .graph
        .neighbors_directed(current_node_idx, petgraph::Direction::Incoming)
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
    current_node_idx: NodeIndex,
    edge_predicate: EP,
    node_predicate: NP,
) -> Option<NodeIndex> {
    let incoming_edges_matching_predicate =
        find_incoming_edges(graph, current_node_idx, edge_predicate);

    for edge_idx in incoming_edges_matching_predicate {
        let Some((source_node_idx, _)) = graph.graph.edge_endpoints(edge_idx) else {
            continue;
        };

        let source_node = &graph.graph[source_node_idx];

        if node_predicate(source_node) {
            return Some(source_node_idx);
        }
    }

    None
}

pub fn get_previous_commit_value_of_collector(
    storage: &DashMap<(CollectorConfig, CommitHash), CollectorValue>,
    graph: &CollectionExecutionGraph,
    current_node_idx: NodeIndex,
) -> Option<CollectorValue> {
    let current_node = &graph.graph[current_node_idx];

    let previous_node_index = find_preceding_node(
        graph,
        current_node_idx,
        |e| e.distance == 1,
        |n| n.collector_config == current_node.collector_config,
    )?;

    let previous_node = &graph.graph[previous_node_index];

    let value = storage.get(&(
        previous_node.collector_config.clone(),
        previous_node.commit_hash.clone(),
    ));

    value.map(|v| v.clone())
}

#[derive(Error, Debug)]
#[error("Could not read required value from storage for node {task_idx:?}")]
pub struct LookupError {
    task_idx: NodeIndex,
}

pub fn get_value_of_preceeding_node<
    EP: Fn(&CollectionGraphEdge) -> bool,
    NP: Fn(&CollectionTask) -> bool,
>(
    storage: &DashMap<(CollectorConfig, CommitHash), CollectorValue>,
    graph: &CollectionExecutionGraph,
    current_node_idx: NodeIndex,
    edge_predicate: EP,
    node_predicate: NP,
) -> Result<CollectorValue, LookupError> {
    let task_idx = find_preceding_node(graph, current_node_idx, edge_predicate, node_predicate)
        .unwrap_or_else(|| {
            panic!("Could not find required dependency task for node {current_node_idx:?}",)
        });

    let task = &graph.graph[task_idx];

    let task_value = storage
        .get(&(task.collector_config.clone(), task.commit_hash.clone()))
        .ok_or(LookupError { task_idx })?;

    Ok(task_value.clone())
}
