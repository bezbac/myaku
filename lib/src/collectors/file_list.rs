use dashmap::DashMap;
use petgraph::graph::NodeIndex;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    config::CollectorConfig,
    git::{CommitHash, GitError, WorktreeHandle},
    graph::CollectionExecutionGraph,
};

use super::{BaseCollector, CollectorValue};

#[derive(Debug)]
pub(crate) struct FileList;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct FileListValue {
    pub files: Vec<String>,
}

#[derive(Error, Debug)]
pub enum FileListError {
    #[error("{0}")]
    Git(#[from] GitError),
}

impl BaseCollector for FileList {
    type Error = FileListError;

    #[tracing::instrument(level = "trace", skip_all)]
    fn collect(
        &self,
        _storage: &DashMap<(CollectorConfig, CommitHash), CollectorValue>,
        repo: &mut WorktreeHandle,
        _graph: &CollectionExecutionGraph,
        _current_node_idx: NodeIndex,
    ) -> Result<CollectorValue, FileListError> {
        let files_at_current_commit = repo.list_files()?;

        let value = FileListValue {
            files: files_at_current_commit,
        };

        Ok(value.into())
    }
}
