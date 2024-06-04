use std::collections::{BTreeMap, HashMap};

use anyhow::Result;
use petgraph::graph::NodeIndex;
use tokei::{LanguageType, Languages};

use crate::{
    config::CollectorConfig,
    git::{CommitHash, RepositoryHandle},
    graph::CollectionExecutionGraph,
};

use super::Collector;

pub(super) struct Loc;

impl Collector for Loc {
    fn collect(
        &self,
        _storage: &HashMap<(CollectorConfig, CommitHash), String>,
        repo: &RepositoryHandle,
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
