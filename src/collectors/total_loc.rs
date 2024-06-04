use std::collections::HashMap;

use anyhow::Result;
use petgraph::graph::NodeIndex;
use tokei::Languages;

use crate::{
    config::CollectorConfig,
    git::{CommitHash, RepositoryHandle},
    graph::CollectionExecutionGraph,
};

use super::Collector;

pub(super) struct TotalLoc;

impl Collector for TotalLoc {
    fn collect(
        &self,
        _storage: &HashMap<(CollectorConfig, CommitHash), String>,
        repo: &RepositoryHandle,
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
