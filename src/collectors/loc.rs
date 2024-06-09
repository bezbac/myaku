use std::collections::BTreeMap;

use anyhow::Result;
use dashmap::DashMap;
use petgraph::graph::NodeIndex;
use tokei::{LanguageType, Languages};

use crate::{
    config::CollectorConfig,
    git::{CommitHash, WorktreeHandle},
    graph::CollectionExecutionGraph,
};

use super::BaseCollector;

pub(super) struct Loc;

impl BaseCollector for Loc {
    fn collect(
        &self,
        _storage: &DashMap<(CollectorConfig, CommitHash), String>,
        repo: &mut WorktreeHandle,
        _graph: &CollectionExecutionGraph,
        _current_node_idx: &NodeIndex,
    ) -> Result<String> {
        let mut languages = Languages::new();
        languages.get_statistics(&[&repo.path], &[".git"], &tokei::Config::default());
        let value: BTreeMap<&LanguageType, usize> = languages
            .iter()
            .map(|(lang, info)| (lang, info.code))
            .filter(|(_, value)| *value > 0)
            .collect();
        let result = serde_json::to_string(&value)?;
        Ok(result)
    }
}
