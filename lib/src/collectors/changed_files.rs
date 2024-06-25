use std::collections::HashSet;

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

pub(super) struct ChangedFiles;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ChangedFilesValue {
    pub files: HashSet<String>,
}

impl BaseCollector for ChangedFiles {
    fn collect(
        &self,
        _storage: &DashMap<(CollectorConfig, CommitHash), CollectorValue>,
        repo: &mut WorktreeHandle,
        _graph: &CollectionExecutionGraph,
        _current_node_idx: &NodeIndex,
    ) -> Result<CollectorValue> {
        let files_changed_in_current_commit = repo.get_current_changed_file_paths()?;
        let value = ChangedFilesValue {
            files: files_changed_in_current_commit,
        };
        Ok(value.into())
    }
}
