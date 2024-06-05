use anyhow::Result;

use dashmap::DashMap;
use petgraph::graph::NodeIndex;

use crate::{
    config::CollectorConfig,
    git::{CommitHash, WorktreeHandle},
    graph::CollectionExecutionGraph,
};

mod loc;
mod pattern_occurences;
mod total_cargo_dependencies;
mod total_diff_stat;
mod total_loc;
mod total_pattern_occurences;
mod utils;

pub trait Collector {
    fn collect(
        &self,
        storage: &DashMap<(CollectorConfig, CommitHash), String>,
        repo: &mut WorktreeHandle,
        graph: &CollectionExecutionGraph,
        current_node_idx: &NodeIndex,
    ) -> Result<String>;
}

impl From<&CollectorConfig> for Box<dyn Collector> {
    fn from(value: &CollectorConfig) -> Self {
        match value {
            CollectorConfig::Loc => Box::new(loc::Loc {}),
            CollectorConfig::TotalLoc => Box::new(total_loc::TotalLoc {}),
            CollectorConfig::TotalDiffStat => Box::new(total_diff_stat::TotalDiffStat {}),
            CollectorConfig::TotalCargoDeps => {
                Box::new(total_cargo_dependencies::TotalCargoDependencies {})
            }
            CollectorConfig::PatternOccurences { pattern } => {
                Box::new(pattern_occurences::PatternOccurences {
                    pattern: pattern.clone(),
                })
            }
            CollectorConfig::TotalPatternOccurences { pattern } => {
                Box::new(total_pattern_occurences::TotalPatternOccurences {
                    pattern: pattern.clone(),
                })
            }
        }
    }
}
