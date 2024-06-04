use std::collections::HashMap;

use anyhow::Result;
use petgraph::graph::NodeIndex;

use crate::{
    config::CollectorConfig,
    git::{CommitHash, RepositoryHandle},
    graph::CollectionExecutionGraph,
};

use super::Collector;

pub(super) struct TotalDiffStat;

impl Collector for TotalDiffStat {
    fn collect(
        &self,
        _storage: &HashMap<(CollectorConfig, CommitHash), String>,
        repo: &RepositoryHandle,
        _graph: &CollectionExecutionGraph,
        _current_node_idx: &NodeIndex,
    ) -> Result<String> {
        let (files_changed, insertions, deletions) = repo.get_current_total_diff_stat().unwrap();

        let result = serde_json::to_string(&(files_changed, insertions, deletions))?;
        Ok(result)
    }
}
