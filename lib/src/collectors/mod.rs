use changed_files::{ChangedFiles, ChangedFilesError};
use changed_files_loc::{ChangedFilesLoc, ChangedFilesLocError};
use dashmap::DashMap;
use file_list::{FileList, FileListError};
use gritql_pattern_occurences::{GritQLPatternOccurences, GritQLPatternOccurencesError};
use loc::{Loc, LocError};
use pattern_occurences::{PatternOccurences, PatternOccurencesError};
use petgraph::graph::NodeIndex;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use total_cargo_dependencies::{TotalCargoDependencies, TotalCargoDependenciesError};
use total_diff_stat::{TotalDiffStat, TotalDiffStatError};
use total_file_count::{TotalFileCount, TotalFileCountError};
use total_loc::{TotalLoc, TotalLocError};
use total_pattern_occurences::{TotalPatternOccurences, TotalPatternOccurencesError};

use crate::{
    config::CollectorConfig,
    git::{CommitHash, WorktreeHandle},
    graph::CollectionExecutionGraph,
};

mod changed_files;
mod changed_files_loc;
mod file_list;
mod gritql_pattern_occurences;
mod loc;
mod pattern_occurences;
mod total_cargo_dependencies;
mod total_diff_stat;
mod total_file_count;
mod total_loc;
mod total_pattern_occurences;
mod utils;

pub use changed_files::ChangedFilesValue;
pub use changed_files_loc::ChangedFilesLocValue;
pub use file_list::FileListValue;
pub use gritql_pattern_occurences::GritQLPatternOccurencesValue;
pub use loc::LocValue;
pub use pattern_occurences::PatternOccurencesValue;
pub use total_cargo_dependencies::TotalCargoDependenciesValue;
pub use total_diff_stat::TotalDiffStatValue;
pub use total_file_count::TotalFileCountValue;
pub use total_loc::TotalLocValue;
pub use total_pattern_occurences::TotalPatternOccurencesValue;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "collector")]
pub enum CollectorValue {
    ChangedFiles(changed_files::ChangedFilesValue),
    Loc(loc::LocValue),
    PatternOccurences(pattern_occurences::PatternOccurencesValue),
    GritQLPatternOccurences(gritql_pattern_occurences::GritQLPatternOccurencesValue),
    TotalCargoDependencies(total_cargo_dependencies::TotalCargoDependenciesValue),
    TotalDiffStat(total_diff_stat::TotalDiffStatValue),
    TotalLoc(total_loc::TotalLocValue),
    TotalPatternOccurences(total_pattern_occurences::TotalPatternOccurencesValue),
    FileList(file_list::FileListValue),
    TotalFileCount(total_file_count::TotalFileCountValue),
    ChangedFilesLoc(changed_files_loc::ChangedFilesLocValue),
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
    gritql_pattern_occurences::GritQLPatternOccurencesValue,
    GritQLPatternOccurences
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
impl_from!(total_file_count::TotalFileCountValue, TotalFileCount);
impl_from!(changed_files_loc::ChangedFilesLocValue, ChangedFilesLoc);

#[derive(Error, Debug)]
#[error("Could not unpack {to} from {from:?}")]
pub struct CollectorValueCastError {
    from: CollectorValue,
    to: String,
}

macro_rules! impl_try_into {
    ($value_type:ty, $variant:ident) => {
        impl std::convert::TryInto<$value_type> for CollectorValue {
            type Error = CollectorValueCastError;

            fn try_into(self) -> std::result::Result<$value_type, Self::Error> {
                match self {
                    CollectorValue::$variant(value) => Ok(value),
                    _ => Err(CollectorValueCastError {
                        from: self,
                        to: stringify!($value_type).to_string(),
                    }),
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
    gritql_pattern_occurences::GritQLPatternOccurencesValue,
    GritQLPatternOccurences
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
impl_try_into!(total_file_count::TotalFileCountValue, TotalFileCount);
impl_try_into!(changed_files_loc::ChangedFilesLocValue, ChangedFilesLoc);

#[derive(Error, Debug)]
pub enum BaseCollectorError {
    #[error("{0}")]
    ChangedFilesLoc(changed_files_loc::ChangedFilesLocError),

    #[error("{0}")]
    ChangedFiles(changed_files::ChangedFilesError),

    #[error("{0}")]
    FileList(file_list::FileListError),

    #[error("{0}")]
    Loc(LocError),

    #[error("{0}")]
    PatternOccurences(pattern_occurences::PatternOccurencesError),

    #[error("{0}")]
    GritQLPatternOccurences(gritql_pattern_occurences::GritQLPatternOccurencesError),

    #[error("{0}")]
    TotalCargoDependencies(total_cargo_dependencies::TotalCargoDependenciesError),

    #[error("{0}")]
    TotalDiffStat(total_diff_stat::TotalDiffStatError),
}

impl From<ChangedFilesLocError> for BaseCollectorError {
    fn from(value: ChangedFilesLocError) -> Self {
        BaseCollectorError::ChangedFilesLoc(value)
    }
}

impl From<ChangedFilesError> for BaseCollectorError {
    fn from(value: ChangedFilesError) -> Self {
        BaseCollectorError::ChangedFiles(value)
    }
}

impl From<FileListError> for BaseCollectorError {
    fn from(value: FileListError) -> Self {
        BaseCollectorError::FileList(value)
    }
}

impl From<LocError> for BaseCollectorError {
    fn from(value: LocError) -> Self {
        BaseCollectorError::Loc(value)
    }
}

impl From<PatternOccurencesError> for BaseCollectorError {
    fn from(value: PatternOccurencesError) -> Self {
        BaseCollectorError::PatternOccurences(value)
    }
}

impl From<GritQLPatternOccurencesError> for BaseCollectorError {
    fn from(value: GritQLPatternOccurencesError) -> Self {
        BaseCollectorError::GritQLPatternOccurences(value)
    }
}

impl From<TotalCargoDependenciesError> for BaseCollectorError {
    fn from(value: TotalCargoDependenciesError) -> Self {
        BaseCollectorError::TotalCargoDependencies(value)
    }
}

impl From<TotalDiffStatError> for BaseCollectorError {
    fn from(value: TotalDiffStatError) -> Self {
        BaseCollectorError::TotalDiffStat(value)
    }
}

#[allow(clippy::enum_variant_names)]
#[derive(Error, Debug)]
pub enum DerivedCollectorError {
    #[error("{0}")]
    TotalFileCount(total_file_count::TotalFileCountError),

    #[error("{0}")]
    TotalLoc(total_loc::TotalLocError),

    #[error("{0}")]
    TotalPatternOccurences(total_pattern_occurences::TotalPatternOccurencesError),
}

impl From<TotalFileCountError> for DerivedCollectorError {
    fn from(value: TotalFileCountError) -> Self {
        DerivedCollectorError::TotalFileCount(value)
    }
}

impl From<TotalLocError> for DerivedCollectorError {
    fn from(value: TotalLocError) -> Self {
        DerivedCollectorError::TotalLoc(value)
    }
}

impl From<TotalPatternOccurencesError> for DerivedCollectorError {
    fn from(value: TotalPatternOccurencesError) -> Self {
        DerivedCollectorError::TotalPatternOccurences(value)
    }
}

pub(crate) enum BaseCollectorObj {
    ChangedFilesLoc(ChangedFilesLoc),
    ChangedFiles(ChangedFiles),
    FileList(FileList),
    Loc(Loc),
    PatternOccurences(PatternOccurences),
    GritQLPatternOccurences(GritQLPatternOccurences),
    TotalCargoDependencies(TotalCargoDependencies),
    TotalDiffStat(TotalDiffStat),
}

impl BaseCollector for BaseCollectorObj {
    type Error = BaseCollectorError;

    fn collect(
        &self,
        storage: &DashMap<(CollectorConfig, CommitHash), CollectorValue>,
        repo: &mut WorktreeHandle,
        graph: &CollectionExecutionGraph,
        current_node_idx: NodeIndex,
    ) -> Result<CollectorValue, Self::Error> {
        match self {
            BaseCollectorObj::ChangedFiles(collector) => collector
                .collect(storage, repo, graph, current_node_idx)
                .map_err(|err| err.into()),
            BaseCollectorObj::ChangedFilesLoc(collector) => collector
                .collect(storage, repo, graph, current_node_idx)
                .map_err(|err| err.into()),
            BaseCollectorObj::FileList(collector) => collector
                .collect(storage, repo, graph, current_node_idx)
                .map_err(|err| err.into()),
            BaseCollectorObj::Loc(collector) => collector
                .collect(storage, repo, graph, current_node_idx)
                .map_err(|err| err.into()),
            BaseCollectorObj::PatternOccurences(collector) => collector
                .collect(storage, repo, graph, current_node_idx)
                .map_err(|err| err.into()),
            BaseCollectorObj::GritQLPatternOccurences(collector) => collector
                .collect(storage, repo, graph, current_node_idx)
                .map_err(|err| err.into()),
            BaseCollectorObj::TotalCargoDependencies(collector) => collector
                .collect(storage, repo, graph, current_node_idx)
                .map_err(|err| err.into()),
            BaseCollectorObj::TotalDiffStat(collector) => collector
                .collect(storage, repo, graph, current_node_idx)
                .map_err(|err| err.into()),
        }
    }
}

#[allow(clippy::enum_variant_names)]
pub(crate) enum DerivedCollectorObj {
    TotalFileCount(TotalFileCount),
    TotalLoc(TotalLoc),
    TotalPatternOccurences(TotalPatternOccurences),
}

impl DerivedCollector for DerivedCollectorObj {
    type Error = DerivedCollectorError;

    fn collect(
        &self,
        storage: &DashMap<(CollectorConfig, CommitHash), CollectorValue>,
        graph: &CollectionExecutionGraph,
        current_node_idx: NodeIndex,
    ) -> Result<CollectorValue, Self::Error> {
        match self {
            DerivedCollectorObj::TotalFileCount(collector) => collector
                .collect(storage, graph, current_node_idx)
                .map_err(|err| err.into()),
            DerivedCollectorObj::TotalLoc(collector) => collector
                .collect(storage, graph, current_node_idx)
                .map_err(|err| err.into()),
            DerivedCollectorObj::TotalPatternOccurences(collector) => collector
                .collect(storage, graph, current_node_idx)
                .map_err(|err| err.into()),
        }
    }
}

pub enum Collector {
    Base(BaseCollectorObj),
    Derived(DerivedCollectorObj),
}

pub trait BaseCollector {
    type Error;

    /// Collects the value for the given collector.
    fn collect(
        &self,
        storage: &DashMap<(CollectorConfig, CommitHash), CollectorValue>,
        repo: &mut WorktreeHandle,
        graph: &CollectionExecutionGraph,
        current_node_idx: NodeIndex,
    ) -> Result<CollectorValue, Self::Error>;
}

pub trait DerivedCollector {
    type Error;

    fn collect(
        &self,
        storage: &DashMap<(CollectorConfig, CommitHash), CollectorValue>,
        graph: &CollectionExecutionGraph,
        current_node_idx: NodeIndex,
    ) -> Result<CollectorValue, Self::Error>;
}

impl From<&CollectorConfig> for Collector {
    fn from(value: &CollectorConfig) -> Self {
        match value {
            CollectorConfig::Loc => Collector::Base(BaseCollectorObj::Loc(loc::Loc {})),
            CollectorConfig::ChangedFiles => Collector::Base(BaseCollectorObj::ChangedFiles(
                changed_files::ChangedFiles {},
            )),
            CollectorConfig::TotalLoc => {
                Collector::Derived(DerivedCollectorObj::TotalLoc(total_loc::TotalLoc {}))
            }
            CollectorConfig::TotalDiffStat => Collector::Base(BaseCollectorObj::TotalDiffStat(
                total_diff_stat::TotalDiffStat {},
            )),
            CollectorConfig::TotalCargoDeps => {
                Collector::Base(BaseCollectorObj::TotalCargoDependencies(
                    total_cargo_dependencies::TotalCargoDependencies {},
                ))
            }
            CollectorConfig::PatternOccurences { pattern, files } => Collector::Base(
                BaseCollectorObj::PatternOccurences(pattern_occurences::PatternOccurences {
                    pattern: pattern.clone(),
                    files: files.clone(),
                }),
            ),
            CollectorConfig::GritQLPatternOccurences { pattern, files } => {
                Collector::Base(BaseCollectorObj::GritQLPatternOccurences(
                    gritql_pattern_occurences::GritQLPatternOccurences {
                        pattern: pattern.clone(),
                        files: files.clone(),
                    },
                ))
            }
            CollectorConfig::TotalPatternOccurences { pattern, files } => {
                Collector::Derived(DerivedCollectorObj::TotalPatternOccurences(
                    total_pattern_occurences::TotalPatternOccurences {
                        pattern: pattern.clone(),
                        files: files.clone(),
                    },
                ))
            }
            CollectorConfig::FileList => {
                Collector::Base(BaseCollectorObj::FileList(file_list::FileList {}))
            }
            CollectorConfig::TotalFileCount => Collector::Derived(
                DerivedCollectorObj::TotalFileCount(total_file_count::TotalFileCount {}),
            ),
            CollectorConfig::ChangedFilesLoc => Collector::Base(BaseCollectorObj::ChangedFilesLoc(
                changed_files_loc::ChangedFilesLoc {},
            )),
        }
    }
}
