use anyhow::Result;
use dashmap::DashMap;
use petgraph::graph::NodeIndex;

use crate::{
    config::CollectorConfig,
    git::{CommitHash, WorktreeHandle},
    graph::CollectionExecutionGraph,
};

use super::BaseCollector;

pub(super) struct ChangedFiles;

impl BaseCollector for ChangedFiles {
    fn collect(
        &self,
        _storage: &DashMap<(CollectorConfig, CommitHash), String>,
        repo: &mut WorktreeHandle,
        _graph: &CollectionExecutionGraph,
        _current_node_idx: &NodeIndex,
    ) -> Result<String> {
        let files_changed_in_current_commit = repo.get_current_changed_file_paths()?;
        let result = serde_json::to_string(&files_changed_in_current_commit)?;
        Ok(result)
    }
}
