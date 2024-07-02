use std::collections::HashMap;

use anyhow::Result;
use petgraph::{graph::NodeIndex, Graph};

use crate::{
    config::{CollectorConfig, MetricConfig},
    git::{CommitHash, CommitInfo},
};

#[derive(PartialEq, Clone, Debug)]
pub struct CollectionTask {
    pub commit_hash: CommitHash,
    pub collector_config: CollectorConfig,
}

#[derive(Clone, Debug)]
pub struct CollectionGraphEdge {
    /// The number of commits between the two nodes
    pub distance: u32,
}

#[derive(Clone, Debug)]
pub struct CollectionExecutionGraph {
    pub graph: Graph<CollectionTask, CollectionGraphEdge>,
}

pub fn add_task(
    graph: &mut Graph<CollectionTask, CollectionGraphEdge>,
    created_tasks: &mut HashMap<(CollectorConfig, CommitHash), NodeIndex>,
    collector_config: &CollectorConfig,
    current_commit_hash: &CommitHash,
    previous_commit_hash: Option<&CommitHash>,
) -> Result<NodeIndex> {
    if let Some(node_idx) =
        created_tasks.get(&(collector_config.clone(), current_commit_hash.clone()))
    {
        return Ok(*node_idx);
    }

    let node_idx = graph.add_node(CollectionTask {
        commit_hash: current_commit_hash.clone(),
        collector_config: collector_config.clone(),
    });

    created_tasks.insert(
        (collector_config.clone(), current_commit_hash.clone()),
        node_idx,
    );

    // Create dependency tasks
    match &collector_config {
        CollectorConfig::TotalPatternOccurences { pattern } => {
            let dependency_node_idx = add_task(
                graph,
                created_tasks,
                &CollectorConfig::PatternOccurences {
                    pattern: pattern.clone(),
                },
                current_commit_hash,
                previous_commit_hash,
            )?;

            graph.add_edge(
                dependency_node_idx,
                node_idx,
                CollectionGraphEdge { distance: 0 },
            );
        }
        CollectorConfig::PatternOccurences { pattern: _ } | CollectorConfig::TotalCargoDeps => {
            let dependency_node_idx = add_task(
                graph,
                created_tasks,
                &CollectorConfig::ChangedFiles,
                current_commit_hash,
                previous_commit_hash,
            )?;

            graph.add_edge(
                dependency_node_idx,
                node_idx,
                CollectionGraphEdge { distance: 0 },
            );
        }
        CollectorConfig::TotalLoc => {
            let dependency_node_idx = add_task(
                graph,
                created_tasks,
                &CollectorConfig::Loc,
                current_commit_hash,
                previous_commit_hash,
            )?;

            graph.add_edge(
                dependency_node_idx,
                node_idx,
                CollectionGraphEdge { distance: 0 },
            );
        }
        _ => {}
    }

    if let Some(previous_commit_hash) = previous_commit_hash {
        if let Some(last_commit_task_idx) =
            created_tasks.get(&(collector_config.clone(), previous_commit_hash.clone()))
        {
            graph.add_edge(
                *last_commit_task_idx,
                node_idx,
                CollectionGraphEdge { distance: 1 },
            );
        }
    }

    Ok(node_idx)
}

pub fn build_collection_execution_graph(
    metrics: &HashMap<String, MetricConfig>,
    commits: &[CommitInfo],
) -> Result<CollectionExecutionGraph> {
    let mut graph: Graph<CollectionTask, CollectionGraphEdge> = Graph::new();

    let mut created_tasks: HashMap<(CollectorConfig, CommitHash), NodeIndex> = HashMap::new();

    for commit_idx in 0..commits.len() {
        let previous_commit = if commit_idx > 0 {
            Some(&commits[commit_idx - 1])
        } else {
            None
        };

        let current_commit = &commits[commit_idx];

        let current_commit_hash = &current_commit.id;

        for metric_config in metrics.values() {
            add_task(
                &mut graph,
                &mut created_tasks,
                &metric_config.collector,
                current_commit_hash,
                previous_commit.map(|c| &c.id),
            )?;
        }
    }

    Ok(CollectionExecutionGraph { graph })
}
