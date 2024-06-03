use std::collections::HashMap;

use anyhow::Result;
use petgraph::{graph::NodeIndex, Graph};

use crate::{
    collectors::Collector,
    config::{CollectorConfig, Frequency, MetricConfig},
    git::{CommitHash, CommitInfo, RepositoryHandle},
};

#[derive(PartialEq, Clone, Debug)]
pub struct CollectionTask {
    pub metric_name: String,
    pub commit_hash: CommitHash,
    pub was_cached: bool,
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

impl CollectionExecutionGraph {
    pub fn run_task(
        &self,
        storage: &mut HashMap<(CollectorConfig, CommitHash), String>,
        node_idx: &NodeIndex,
        repo: &RepositoryHandle,
    ) -> Result<String> {
        let task = &self.graph[*node_idx];
        let collector: Box<dyn Collector> = (&task.collector_config).into();
        let data = collector.collect(storage, repo, self, node_idx)?;
        storage.insert(
            (task.collector_config.clone(), task.commit_hash.clone()),
            data.clone(),
        );
        Ok(data)
    }
}

pub fn add_task(
    graph: &mut Graph<CollectionTask, CollectionGraphEdge>,
    storage: &mut HashMap<(CollectorConfig, CommitHash), String>,
    created_tasks: &mut HashMap<(String, MetricConfig, CommitHash), NodeIndex>,
    metric_name: &str,
    metric_config: &MetricConfig,
    current_commit_hash: &CommitHash,
    previous_commit_hash: Option<&CommitHash>,
) -> Result<NodeIndex> {
    let cached = storage.get(&(metric_config.collector.clone(), current_commit_hash.clone()));

    let was_cached = cached.is_some();

    let node_idx = graph.add_node(CollectionTask {
        metric_name: metric_name.to_string(),
        commit_hash: current_commit_hash.clone(),
        was_cached,
        collector_config: metric_config.collector.clone(),
    });

    created_tasks.insert(
        (
            metric_name.to_string(),
            metric_config.clone(),
            current_commit_hash.clone(),
        ),
        node_idx,
    );

    // Create dependency tasks
    match &metric_config.collector {
        CollectorConfig::TotalPatternOccurences { pattern } => {
            let dependency_node_idx = add_task(
                graph,
                storage,
                created_tasks,
                format!("{metric_name}_derived_pattern_occurences").as_str(),
                &MetricConfig {
                    collector: CollectorConfig::PatternOccurences {
                        pattern: pattern.clone(),
                    },
                    frequency: Frequency::PerCommit,
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
        _ => {}
    }

    if let Some(previous_commit_hash) = previous_commit_hash {
        if let Some(last_commit_task_idx) = created_tasks.get(&(
            metric_name.to_string(),
            metric_config.clone(),
            previous_commit_hash.clone(),
        )) {
            graph.add_edge(
                last_commit_task_idx.clone(),
                node_idx,
                CollectionGraphEdge { distance: 1 },
            );
        }
    }

    Ok(node_idx)
}

pub fn build_collection_execution_graph(
    storage: &mut HashMap<(CollectorConfig, CommitHash), String>,
    metrics: &HashMap<String, MetricConfig>,
    commits: &[CommitInfo],
) -> Result<CollectionExecutionGraph> {
    let mut graph: Graph<CollectionTask, CollectionGraphEdge> = Graph::new();

    let mut created_tasks: HashMap<(String, MetricConfig, CommitHash), NodeIndex> = HashMap::new();

    for commit_idx in 0..commits.len() {
        let previous_commit = if commit_idx > 0 {
            Some(&commits[commit_idx - 1])
        } else {
            None
        };

        let current_commit = &commits[commit_idx];

        let current_commit_hash = &current_commit.id;

        for (metric_name, metric_config) in metrics {
            add_task(
                &mut graph,
                storage,
                &mut created_tasks,
                metric_name,
                metric_config,
                current_commit_hash,
                previous_commit.map(|c| &c.id),
            )?;
        }
    }

    Ok(CollectionExecutionGraph { graph })
}
