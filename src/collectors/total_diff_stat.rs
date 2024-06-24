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

pub(super) struct TotalDiffStat;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct TotalDiffStatValue {
    pub files_changed: u32,
    pub insertions: u32,
    pub deletions: u32,
}

impl BaseCollector for TotalDiffStat {
    fn collect(
        &self,
        _storage: &DashMap<(CollectorConfig, CommitHash), CollectorValue>,
        repo: &mut WorktreeHandle,
        _graph: &CollectionExecutionGraph,
        _current_node_idx: &NodeIndex,
    ) -> Result<CollectorValue> {
        let (files_changed, insertions, deletions) = repo.get_current_total_diff_stat().unwrap();

        let value = TotalDiffStatValue {
            files_changed: files_changed as u32,
            insertions: insertions as u32,
            deletions: deletions as u32,
        };

        Ok(value.into())
    }
}
