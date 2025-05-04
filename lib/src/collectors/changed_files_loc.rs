use std::collections::HashMap;

use dashmap::DashMap;
use petgraph::graph::NodeIndex;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokei::LanguageType;
use tracing::warn;

use crate::{
    collectors::{utils::get_value_of_preceeding_node, ChangedFilesValue},
    config::CollectorConfig,
    git::{CommitHash, WorktreeHandle},
    graph::CollectionExecutionGraph,
};

use super::{utils::LookupError, BaseCollector, CollectorValue, CollectorValueCastError};

#[derive(Debug)]
pub(crate) struct ChangedFilesLoc;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ChangedFilesLocValue {
    pub files: HashMap<String, Option<usize>>,
}

#[derive(Error, Debug)]
pub enum ChangedFilesLocError {
    #[error("{0}")]
    Lookup(#[from] LookupError),

    #[error("{0}")]
    Cast(#[from] CollectorValueCastError),
}

impl BaseCollector for ChangedFilesLoc {
    type Error = ChangedFilesLocError;

    #[tracing::instrument(level = "trace", skip_all)]
    fn collect(
        &self,
        storage: &DashMap<(CollectorConfig, CommitHash), CollectorValue>,
        repo: &mut WorktreeHandle,
        graph: &CollectionExecutionGraph,
        current_node_idx: NodeIndex,
    ) -> Result<CollectorValue, ChangedFilesLocError> {
        let changed_files_in_current_commit_value: ChangedFilesValue =
            get_value_of_preceeding_node(
                storage,
                graph,
                current_node_idx,
                |e| e.distance == 0,
                |n| n.collector_config == CollectorConfig::ChangedFiles,
            )?
            .try_into()?;

        let changed_files_in_current_commit = changed_files_in_current_commit_value.files;

        let config = tokei::Config::default();

        // TODO: Does it make sense to use rayon here?
        let reports = changed_files_in_current_commit
            .into_iter()
            .map(|changed_file_relative_path| -> (String, Option<usize>) {
                // Get the absolute path of the file
                let path = repo.path.join(&changed_file_relative_path);

                let language = LanguageType::from_path(&path, &config);

                let Some(language) = language else {
                    return (changed_file_relative_path, None);
                };

                let result = language.parse(path, &config);

                match result {
                    Ok(report) => (
                        changed_file_relative_path,
                        Some(report.stats.summarise().lines()),
                    ),
                    Err(_) => (changed_file_relative_path, None),
                }
            })
            .collect();

        let value = ChangedFilesLocValue { files: reports };

        Ok(value.into())
    }
}
