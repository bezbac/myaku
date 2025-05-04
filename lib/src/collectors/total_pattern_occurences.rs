use dashmap::DashMap;
use globset::Glob;
use petgraph::graph::NodeIndex;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{config::CollectorConfig, git::CommitHash, graph::CollectionExecutionGraph};

use super::{
    pattern_occurences::PatternOccurencesValue,
    utils::{get_value_of_preceeding_node, LookupError},
    CollectorValue, CollectorValueCastError, DerivedCollector,
};

#[derive(Debug)]
pub(crate) struct TotalPatternOccurences {
    pub pattern: String,
    pub files: Option<Vec<Glob>>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct TotalPatternOccurencesValue {
    total_occurences: u32,
}

#[derive(Error, Debug)]
pub enum TotalPatternOccurencesError {
    #[error("{0}")]
    Lookup(#[from] LookupError),

    #[error("{0}")]
    Cast(#[from] CollectorValueCastError),

    #[error("{0}")]
    TryFromIntError(#[from] std::num::TryFromIntError),
}

impl DerivedCollector for TotalPatternOccurences {
    type Error = TotalPatternOccurencesError;

    #[tracing::instrument(level = "trace", skip_all)]
    fn collect(
        &self,
        storage: &DashMap<(CollectorConfig, CommitHash), CollectorValue>,
        graph: &CollectionExecutionGraph,
        current_node_idx: NodeIndex,
    ) -> Result<CollectorValue, TotalPatternOccurencesError> {
        let pattern_occurences_value: PatternOccurencesValue = get_value_of_preceeding_node(
            storage,
            graph,
            current_node_idx,
            |e| e.distance == 0,
            |n| {
                n.collector_config
                    == CollectorConfig::PatternOccurences {
                        pattern: self.pattern.clone(),
                        files: self.files.clone(),
                    }
            },
        )?
        .try_into()?;

        let value = TotalPatternOccurencesValue {
            total_occurences: u32::try_from(pattern_occurences_value.matches.len())?,
        };

        Ok(value.into())
    }
}
