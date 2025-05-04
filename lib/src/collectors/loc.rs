use std::collections::BTreeMap;

use dashmap::DashMap;
use petgraph::graph::NodeIndex;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokei::{LanguageType, Languages};

use crate::{
    config::CollectorConfig,
    git::{CommitHash, GitError, WorktreeHandle},
    graph::CollectionExecutionGraph,
};

use super::{BaseCollector, CollectorValue};

#[derive(Debug)]
pub(crate) struct Loc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocValue {
    pub loc_by_language: BTreeMap<LanguageType, usize>,
}

#[derive(Error, Debug)]
pub enum LocError {
    #[error("{0}")]
    Git(#[from] GitError),
}

impl BaseCollector for Loc {
    type Error = LocError;

    #[tracing::instrument(level = "trace", skip_all)]
    fn collect(
        &self,
        _storage: &DashMap<(CollectorConfig, CommitHash), CollectorValue>,
        repo: &mut WorktreeHandle,
        _graph: &CollectionExecutionGraph,
        _current_node_idx: NodeIndex,
    ) -> Result<CollectorValue, LocError> {
        let mut languages = Languages::new();
        languages.get_statistics(&[&repo.path], &[".git"], &tokei::Config::default());
        let value: BTreeMap<LanguageType, usize> = languages
            .iter()
            .map(|(lang, info)| (*lang, info.code))
            .filter(|(_, value)| *value > 0)
            .collect();

        let value = LocValue {
            loc_by_language: value,
        };

        Ok(value.into())
    }
}
