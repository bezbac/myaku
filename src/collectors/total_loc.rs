use anyhow::Result;
use dashmap::DashMap;
use petgraph::graph::NodeIndex;
use tokei::Languages;

use crate::{
    config::CollectorConfig,
    git::{CommitHash, WorktreeHandle},
    graph::CollectionExecutionGraph,
};

use super::Collector;

#[derive(Debug)]
pub(super) struct TotalLoc;

impl Collector for TotalLoc {
    #[tracing::instrument(level = "trace", skip_all)]
    fn collect(
        &self,
        _storage: &DashMap<(CollectorConfig, CommitHash), String>,
        repo: &mut WorktreeHandle,
        _graph: &CollectionExecutionGraph,
        _current_node_idx: &NodeIndex,
    ) -> Result<String> {
        let mut languages = Languages::new();
        languages.get_statistics(&[&repo.path], &[".git"], &tokei::Config::default());
        let value = languages.total().code;
        let result = serde_json::to_string(&value)?;
        Ok(result)
    }
}
