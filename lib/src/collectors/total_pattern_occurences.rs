use anyhow::Result;
use dashmap::DashMap;
use petgraph::graph::NodeIndex;
use serde::{Deserialize, Serialize};

use crate::{config::CollectorConfig, git::CommitHash, graph::CollectionExecutionGraph};

use super::{
    pattern_occurences::PatternOccurencesValue, utils::get_value_of_preceeding_node,
    CollectorValue, DerivedCollector,
};

#[derive(Debug)]
pub(super) struct TotalPatternOccurences {
    pub pattern: String,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct TotalPatternOccurencesValue {
    total_occurences: u32,
}

impl DerivedCollector for TotalPatternOccurences {
    #[tracing::instrument(level = "trace", skip_all)]
    fn collect(
        &self,
        storage: &DashMap<(CollectorConfig, CommitHash), CollectorValue>,
        graph: &CollectionExecutionGraph,
        current_node_idx: NodeIndex,
    ) -> Result<CollectorValue> {
        let pattern_occurences_value: PatternOccurencesValue = get_value_of_preceeding_node(
            storage,
            graph,
            current_node_idx,
            |e| e.distance == 0,
            |n| {
                n.collector_config
                    == CollectorConfig::PatternOccurences {
                        pattern: self.pattern.clone(),
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
