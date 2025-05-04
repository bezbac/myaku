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
pub(crate) struct TotalDiffStat;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct TotalDiffStatValue {
    pub files_changed: u32,
    pub insertions: u32,
    pub deletions: u32,
}

#[derive(Error, Debug)]
pub enum TotalDiffStatError {
    #[error("{0}")]
    Git(#[from] GitError),

    #[error("{0}")]
    TryFromIntError(#[from] std::num::TryFromIntError),
}

impl BaseCollector for TotalDiffStat {
    type Error = TotalDiffStatError;

    #[tracing::instrument(level = "trace", skip_all)]
    fn collect(
        &self,
        _storage: &DashMap<(CollectorConfig, CommitHash), CollectorValue>,
        repo: &mut WorktreeHandle,
        _graph: &CollectionExecutionGraph,
        _current_node_idx: NodeIndex,
    ) -> Result<CollectorValue, TotalDiffStatError> {
        let (files_changed, insertions, deletions) = repo.get_current_total_diff_stat()?;

        let value = TotalDiffStatValue {
            files_changed: u32::try_from(files_changed)?,
            insertions: u32::try_from(insertions)?,
            deletions: u32::try_from(deletions)?,
        };

        Ok(value.into())
    }
}
