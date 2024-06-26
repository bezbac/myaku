use anyhow::Result;
use dashmap::DashMap;
use petgraph::graph::NodeIndex;
use serde::{Deserialize, Serialize};

use crate::{config::CollectorConfig, git::CommitHash, graph::CollectionExecutionGraph};

use super::{loc::LocValue, utils::get_value_of_preceeding_node, CollectorValue, DerivedCollector};

#[derive(Debug)]
pub(super) struct TotalLoc;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct TotalLocValue {
    loc: u32,
}

impl DerivedCollector for TotalLoc {
    #[tracing::instrument(level = "trace", skip_all)]
    fn collect(
        &self,
        storage: &DashMap<(CollectorConfig, CommitHash), CollectorValue>,
        graph: &CollectionExecutionGraph,
        current_node_idx: &NodeIndex,
    ) -> Result<CollectorValue> {
        let loc_value: LocValue = get_value_of_preceeding_node(
            storage,
            graph,
            current_node_idx,
            |e| e.distance == 0,
            |n| n.collector_config == CollectorConfig::Loc,
        )?
        .try_into()?;

        let value = TotalLocValue {
            loc: loc_value.loc_by_language.values().sum::<usize>() as u32,
        };

        Ok(value.into())
    }
}
