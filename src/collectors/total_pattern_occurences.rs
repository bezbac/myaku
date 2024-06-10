use std::collections::HashSet;

use anyhow::Result;
use dashmap::DashMap;
use petgraph::graph::NodeIndex;

use crate::{config::CollectorConfig, git::CommitHash, graph::CollectionExecutionGraph};

use super::{
    pattern_occurences::PartialMatchData, utils::get_value_of_preceeding_node, DerivedCollector,
};

pub(super) struct TotalPatternOccurences {
    pub pattern: String,
}

impl DerivedCollector for TotalPatternOccurences {
    fn collect(
        &self,
        storage: &DashMap<(CollectorConfig, CommitHash), String>,
        graph: &CollectionExecutionGraph,
        current_node_idx: &NodeIndex,
    ) -> Result<String> {
        let pattern_occurences_value = get_value_of_preceeding_node(
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
        )?;

        let matches: HashSet<PartialMatchData> = serde_json::from_str(&pattern_occurences_value)?;

        let total_matches = matches.len();

        let result = serde_json::to_string(&total_matches)?;

        return Ok(result);
    }
}
