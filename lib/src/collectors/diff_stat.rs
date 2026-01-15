use std::{collections::HashMap, path::PathBuf};

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
pub(crate) struct DiffStat;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct PerFileDiffStat {
    pub insertions: usize,
    pub deletions: usize,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct DiffStatValue {
    pub files: HashMap<PathBuf, PerFileDiffStat>,
}

#[derive(Error, Debug)]
pub enum DiffStatError {
    #[error("{0}")]
    Git(#[from] GitError),

    #[error("{0}")]
    TryFromIntError(#[from] std::num::TryFromIntError),
}

impl BaseCollector for DiffStat {
    type Error = DiffStatError;

    #[tracing::instrument(level = "trace", skip_all)]
    fn collect(
        &self,
        _storage: &DashMap<(CollectorConfig, CommitHash), CollectorValue>,
        repo: &mut WorktreeHandle,
        _graph: &CollectionExecutionGraph,
        _current_node_idx: NodeIndex,
    ) -> Result<CollectorValue, DiffStatError> {
        let per_file_diff_stat = repo.get_current_per_file_diff_stat()?;

        let files = per_file_diff_stat
            .into_iter()
            .map(|(path, (insertions, deletions))| {
                (
                    path,
                    PerFileDiffStat {
                        insertions,
                        deletions,
                    },
                )
            })
            .collect();

        Ok(CollectorValue::DiffStat(DiffStatValue { files }))
    }
}
