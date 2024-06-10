use std::collections::BTreeMap;

use anyhow::Result;
use dashmap::DashMap;
use petgraph::graph::NodeIndex;

use crate::{config::CollectorConfig, git::CommitHash, graph::CollectionExecutionGraph};

use super::{utils::get_value_of_preceeding_node, DerivedCollector};

pub(super) struct TotalLoc;

impl DerivedCollector for TotalLoc {
    fn collect(
        &self,
        storage: &DashMap<(CollectorConfig, CommitHash), String>,
        graph: &CollectionExecutionGraph,
        current_node_idx: &NodeIndex,
    ) -> Result<String> {
        let loc_value = get_value_of_preceeding_node(
            storage,
            graph,
            current_node_idx,
            |e| e.distance == 0,
            |n| n.collector_config == CollectorConfig::Loc,
        )?;

        let loc: BTreeMap<String, usize> = serde_json::from_str(&loc_value)?;
        let value: usize = loc.values().sum();
        let result = serde_json::to_string(&value)?;
        Ok(result)
    }
}
