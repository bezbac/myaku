use std::collections::HashSet;

use anyhow::Result;
use dashmap::DashMap;
use petgraph::graph::NodeIndex;

use crate::{
    config::CollectorConfig,
    git::{CommitHash, WorktreeHandle},
    graph::CollectionExecutionGraph,
};

use super::{pattern_occurences::PartialMatchData, utils::find_preceding_node, Collector};

pub(super) struct TotalPatternOccurences {
    pub pattern: String,
}

impl Collector for TotalPatternOccurences {
    fn collect(
        &self,
        storage: &DashMap<(CollectorConfig, CommitHash), String>,
        _repo: &mut WorktreeHandle,
        graph: &CollectionExecutionGraph,
        current_node_idx: &NodeIndex,
    ) -> Result<String> {
        let pattern_occurences_task_idx = find_preceding_node(
            graph,
            current_node_idx,
            |e| e.distance == 0,
            |n| {
                n.collector_config
                    == CollectorConfig::PatternOccurences {
                        pattern: self.pattern.clone(),
                    }
            },
        )
        .unwrap_or_else(|| {
            panic!(
                "Could not find required dependency task for node {:?}",
                current_node_idx
            )
        });

        let pattern_occurences_task = &graph.graph[pattern_occurences_task_idx];

        let pattern_occurences_value = storage
            .get(&(
                pattern_occurences_task.collector_config.clone(),
                pattern_occurences_task.commit_hash.clone(),
            ))
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Could not read required value from storage for node {:?}",
                    pattern_occurences_task_idx
                )
            })?;

        let matches: HashSet<PartialMatchData> = serde_json::from_str(&pattern_occurences_value)?;

        let total_matches = matches.len();

        let result = serde_json::to_string(&total_matches)?;

        return Ok(result);
    }
}
