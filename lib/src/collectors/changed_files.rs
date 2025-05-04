use std::collections::HashSet;

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
pub(crate) struct ChangedFiles;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ChangedFilesValue {
    pub files: HashSet<String>,
}

#[derive(Error, Debug)]
pub enum ChangedFilesError {
    #[error("{0}")]
    Git(#[from] GitError),
}

impl BaseCollector for ChangedFiles {
    type Error = ChangedFilesError;

    #[tracing::instrument(level = "trace", skip_all)]
    fn collect(
        &self,
        _storage: &DashMap<(CollectorConfig, CommitHash), CollectorValue>,
        repo: &mut WorktreeHandle,
        _graph: &CollectionExecutionGraph,
        _current_node_idx: NodeIndex,
    ) -> Result<CollectorValue, ChangedFilesError> {
        let files_changed_in_current_commit = repo.get_current_changed_file_paths()?;
        let value = ChangedFilesValue {
            files: files_changed_in_current_commit,
        };
        Ok(value.into())
    }
}
