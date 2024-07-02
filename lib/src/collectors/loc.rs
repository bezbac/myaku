use std::collections::BTreeMap;

use anyhow::Result;
use dashmap::DashMap;
use petgraph::graph::NodeIndex;
use serde::{Deserialize, Serialize};
use tokei::{LanguageType, Languages};

use crate::{
    config::CollectorConfig,
    git::{CommitHash, WorktreeHandle},
    graph::CollectionExecutionGraph,
};

use super::{BaseCollector, CollectorValue};

#[derive(Debug)]
pub(super) struct Loc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocValue {
    pub loc_by_language: BTreeMap<LanguageType, usize>,
}

impl BaseCollector for Loc {
    #[tracing::instrument(level = "trace", skip_all)]
    fn collect(
        &self,
        _storage: &DashMap<(CollectorConfig, CommitHash), CollectorValue>,
        repo: &mut WorktreeHandle,
        _graph: &CollectionExecutionGraph,
        _current_node_idx: NodeIndex,
    ) -> Result<CollectorValue> {
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
