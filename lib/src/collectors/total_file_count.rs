use anyhow::Result;
use dashmap::DashMap;
use petgraph::graph::NodeIndex;
use serde::{Deserialize, Serialize};

use crate::{config::CollectorConfig, git::CommitHash, graph::CollectionExecutionGraph};

use super::{utils::get_value_of_preceeding_node, CollectorValue, DerivedCollector, FileListValue};

#[derive(Debug)]
pub(super) struct TotalFileCount;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TotalFileCountValue {
    total_file_count: u32,
}

impl DerivedCollector for TotalFileCount {
    #[tracing::instrument(level = "trace", skip_all)]
    fn collect(
        &self,
        storage: &DashMap<(CollectorConfig, CommitHash), CollectorValue>,
        graph: &CollectionExecutionGraph,
        current_node_idx: NodeIndex,
    ) -> Result<CollectorValue> {
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
