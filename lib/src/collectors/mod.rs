use anyhow::Result;

use dashmap::DashMap;
use petgraph::graph::NodeIndex;
use serde::{Deserialize, Serialize};

use crate::{
    config::CollectorConfig,
    git::{CommitHash, WorktreeHandle},
    graph::CollectionExecutionGraph,
};

mod changed_files;
mod file_list;
mod loc;
mod pattern_occurences;
mod total_cargo_dependencies;
mod total_diff_stat;
mod total_loc;
mod total_pattern_occurences;
mod utils;

pub use changed_files::ChangedFilesValue;
pub use file_list::FileListValue;
pub use loc::LocValue;
pub use pattern_occurences::PatternOccurencesValue;
pub use total_cargo_dependencies::TotalCargoDependenciesValue;
pub use total_diff_stat::TotalDiffStatValue;
pub use total_loc::TotalLocValue;
pub use total_pattern_occurences::TotalPatternOccurencesValue;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "collector")]
pub enum CollectorValue {
    ChangedFiles(changed_files::ChangedFilesValue),
    Loc(loc::LocValue),
    PatternOccurences(pattern_occurences::PatternOccurencesValue),
    TotalCargoDependencies(total_cargo_dependencies::TotalCargoDependenciesValue),
    TotalDiffStat(total_diff_stat::TotalDiffStatValue),
    TotalLoc(total_loc::TotalLocValue),
    TotalPatternOccurences(total_pattern_occurences::TotalPatternOccurencesValue),
    FileList(file_list::FileListValue),
}

macro_rules! impl_from {
    ($value_type:ty, $variant:ident) => {
        impl From<$value_type> for CollectorValue {
            fn from(value: $value_type) -> Self {
                Self::$variant(value)
            }
        }
    };
}

impl_from!(changed_files::ChangedFilesValue, ChangedFiles);
impl_from!(loc::LocValue, Loc);
impl_from!(
    pattern_occurences::PatternOccurencesValue,
    PatternOccurences
);
impl_from!(
    total_cargo_dependencies::TotalCargoDependenciesValue,
    TotalCargoDependencies
);
impl_from!(total_diff_stat::TotalDiffStatValue, TotalDiffStat);
impl_from!(total_loc::TotalLocValue, TotalLoc);
impl_from!(
    total_pattern_occurences::TotalPatternOccurencesValue,
    TotalPatternOccurences
);
impl_from!(file_list::FileListValue, FileList);

macro_rules! impl_try_into {
    ($value_type:ty, $variant:ident) => {
        impl std::convert::TryInto<$value_type> for CollectorValue {
            type Error = anyhow::Error;

            fn try_into(self) -> std::result::Result<$value_type, Self::Error> {
                match self {
                    CollectorValue::$variant(value) => Ok(value),
                    _ => Err(anyhow::anyhow!(
                        "Cannot unpack {} from {:?}",
                        stringify!($value_type),
                        self
                    )),
                }
            }
        }
    };
}

impl_try_into!(changed_files::ChangedFilesValue, ChangedFiles);
impl_try_into!(loc::LocValue, Loc);
impl_try_into!(
    pattern_occurences::PatternOccurencesValue,
    PatternOccurences
);
impl_try_into!(
    total_cargo_dependencies::TotalCargoDependenciesValue,
    TotalCargoDependencies
);
impl_try_into!(total_diff_stat::TotalDiffStatValue, TotalDiffStat);
impl_try_into!(total_loc::TotalLocValue, TotalLoc);
impl_try_into!(
    total_pattern_occurences::TotalPatternOccurencesValue,
    TotalPatternOccurences
);
impl_try_into!(file_list::FileListValue, FileList);

pub enum Collector {
    Base(Box<dyn BaseCollector>),
    Derived(Box<dyn DerivedCollector>),
}

pub trait BaseCollector {
    fn collect(
        &self,
        storage: &DashMap<(CollectorConfig, CommitHash), CollectorValue>,
        repo: &mut WorktreeHandle,
        graph: &CollectionExecutionGraph,
        current_node_idx: NodeIndex,
    ) -> Result<CollectorValue>;
}

pub trait DerivedCollector {
    fn collect(
        &self,
        storage: &DashMap<(CollectorConfig, CommitHash), CollectorValue>,
        graph: &CollectionExecutionGraph,
        current_node_idx: NodeIndex,
    ) -> Result<CollectorValue>;
}

impl From<&CollectorConfig> for Collector {
    fn from(value: &CollectorConfig) -> Self {
        match value {
            CollectorConfig::Loc => Collector::Base(Box::new(loc::Loc {})),
            CollectorConfig::ChangedFiles => {
                Collector::Base(Box::new(changed_files::ChangedFiles {}))
            }
            CollectorConfig::TotalLoc => Collector::Derived(Box::new(total_loc::TotalLoc {})),
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
            CollectorConfig::FileList => Collector::Base(Box::new(file_list::FileList {})),
        }
    }
}
