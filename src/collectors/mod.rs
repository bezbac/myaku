use anyhow::Result;

use dashmap::DashMap;
use petgraph::graph::NodeIndex;

use crate::{
    config::CollectorConfig,
    git::{CommitHash, WorktreeHandle},
    graph::CollectionExecutionGraph,
};

mod changed_files;
mod loc;
mod pattern_occurences;
mod total_cargo_dependencies;
mod total_diff_stat;
mod total_loc;
mod total_pattern_occurences;
mod utils;

pub enum Collector {
    Base(Box<dyn BaseCollector>),
    Derived(Box<dyn DerivedCollector>),
}

pub trait BaseCollector {
    fn collect(
        &self,
        storage: &DashMap<(CollectorConfig, CommitHash), String>,
        repo: &mut WorktreeHandle,
        graph: &CollectionExecutionGraph,
        current_node_idx: &NodeIndex,
    ) -> Result<String>;
}

pub trait DerivedCollector {
    fn collect(
        &self,
        storage: &DashMap<(CollectorConfig, CommitHash), String>,
        graph: &CollectionExecutionGraph,
        current_node_idx: &NodeIndex,
    ) -> Result<String>;
}

impl From<&CollectorConfig> for Collector {
    fn from(value: &CollectorConfig) -> Self {
        match value {
            CollectorConfig::Loc => Collector::Base(Box::new(loc::Loc {})),
            CollectorConfig::ChangedFiles => {
                Collector::Base(Box::new(changed_files::ChangedFiles {}))
            }
            CollectorConfig::TotalLoc => Collector::Base(Box::new(total_loc::TotalLoc {})),
            CollectorConfig::TotalDiffStat => {
                Collector::Base(Box::new(total_diff_stat::TotalDiffStat {}))
            }
            CollectorConfig::TotalCargoDeps => Collector::Base(Box::new(
                total_cargo_dependencies::TotalCargoDependencies {},
            )),
            CollectorConfig::PatternOccurences { pattern } => {
                Collector::Base(Box::new(pattern_occurences::PatternOccurences {
                    pattern: pattern.clone(),
                }))
            }
            CollectorConfig::TotalPatternOccurences { pattern } => {
                Collector::Derived(Box::new(total_pattern_occurences::TotalPatternOccurences {
                    pattern: pattern.clone(),
                }))
            }
        }
    }
}
