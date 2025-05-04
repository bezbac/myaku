use dashmap::DashMap;
use petgraph::graph::NodeIndex;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{config::CollectorConfig, git::CommitHash, graph::CollectionExecutionGraph};

use super::{
    utils::{get_value_of_preceeding_node, LookupError},
    CollectorValue, CollectorValueCastError, DerivedCollector, FileListValue,
};

#[derive(Debug)]
pub(crate) struct TotalFileCount;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TotalFileCountValue {
    total_file_count: u32,
}

#[derive(Error, Debug)]
pub enum TotalFileCountError {
    #[error("{0}")]
    Lookup(#[from] LookupError),

    #[error("{0}")]
    Cast(#[from] CollectorValueCastError),

    #[error("{0}")]
    TryFromIntError(#[from] std::num::TryFromIntError),
}

impl DerivedCollector for TotalFileCount {
    type Error = TotalFileCountError;

    #[tracing::instrument(level = "trace", skip_all)]
    fn collect(
        &self,
        storage: &DashMap<(CollectorConfig, CommitHash), CollectorValue>,
        graph: &CollectionExecutionGraph,
        current_node_idx: NodeIndex,
    ) -> Result<CollectorValue, TotalFileCountError> {
        let file_list_value: FileListValue = get_value_of_preceeding_node(
            storage,
            graph,
            current_node_idx,
            |e| e.distance == 0,
            |n| n.collector_config == CollectorConfig::FileList,
        )?
        .try_into()?;

        let value = TotalFileCountValue {
            total_file_count: u32::try_from(file_list_value.files.len())?,
        };

        Ok(value.into())
    }
}
