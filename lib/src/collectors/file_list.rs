use anyhow::Result;
use dashmap::DashMap;
use petgraph::graph::NodeIndex;
use serde::{Deserialize, Serialize};

use crate::{
    config::CollectorConfig,
    git::{CommitHash, WorktreeHandle},
    graph::CollectionExecutionGraph,
};

use super::{BaseCollector, CollectorValue};

#[derive(Debug)]
pub(super) struct FileList;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct FileListValue {
    pub files: Vec<String>,
}

impl BaseCollector for FileList {
    #[tracing::instrument(level = "trace", skip_all)]
    fn collect(
        &self,
        _storage: &DashMap<(CollectorConfig, CommitHash), CollectorValue>,
        repo: &mut WorktreeHandle,
        _graph: &CollectionExecutionGraph,
        _current_node_idx: NodeIndex,
    ) -> Result<CollectorValue> {
        let files_at_current_commit = repo.list_files()?;

        let value = FileListValue {
            files: files_at_current_commit,
        };

        Ok(value.into())
    }
}
