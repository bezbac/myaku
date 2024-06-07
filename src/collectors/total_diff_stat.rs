use anyhow::Result;
use dashmap::DashMap;
use petgraph::graph::NodeIndex;

use crate::{
    config::CollectorConfig,
    git::{CommitHash, WorktreeHandle},
    graph::CollectionExecutionGraph,
};

use super::Collector;

#[derive(Debug)]
pub(super) struct TotalDiffStat;

impl Collector for TotalDiffStat {
    #[tracing::instrument(level = "trace", skip_all)]
    fn collect(
        &self,
        _storage: &DashMap<(CollectorConfig, CommitHash), String>,
        repo: &mut WorktreeHandle,
        _graph: &CollectionExecutionGraph,
        _current_node_idx: &NodeIndex,
    ) -> Result<String> {
        let (files_changed, insertions, deletions) = repo.get_current_total_diff_stat().unwrap();

        let result = serde_json::to_string(&(files_changed, insertions, deletions))?;
        Ok(result)
    }
}
