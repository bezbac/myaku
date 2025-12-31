use dashmap::DashMap;
use petgraph::graph::NodeIndex;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{config::CollectorConfig, git::CommitHash, graph::CollectionExecutionGraph};

use super::{
    loc::LocValue,
    utils::{get_value_of_preceeding_node, LookupError},
    CollectorValue, CollectorValueCastError, DerivedCollector,
};

#[derive(Debug)]
pub(crate) struct TotalLoc;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct TotalLocValue {
    pub loc: u32,
}

#[derive(Error, Debug)]
pub enum TotalLocError {
    #[error("{0}")]
    Lookup(#[from] LookupError),

    #[error("{0}")]
    Cast(#[from] CollectorValueCastError),

    #[error("{0}")]
    TryFromIntError(#[from] std::num::TryFromIntError),
}

impl DerivedCollector for TotalLoc {
    type Error = TotalLocError;

    #[tracing::instrument(level = "trace", skip_all)]
    fn collect(
        &self,
        storage: &DashMap<(CollectorConfig, CommitHash), CollectorValue>,
        graph: &CollectionExecutionGraph,
        current_node_idx: NodeIndex,
    ) -> Result<CollectorValue, TotalLocError> {
        let loc_value: LocValue = get_value_of_preceeding_node(
            storage,
            graph,
            current_node_idx,
            |e| e.distance == 0,
            |n| n.collector_config == CollectorConfig::Loc,
        )?
        .try_into()?;

        let value = TotalLocValue {
            loc: u32::try_from(loc_value.loc_by_language.values().sum::<usize>())?,
        };

        Ok(value.into())
    }
}
