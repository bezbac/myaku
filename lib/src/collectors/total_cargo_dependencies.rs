use std::{
    collections::HashSet,
    fs::File,
    io::{BufReader, Read},
};

use cargo_lock::Lockfile;
use dashmap::DashMap;
use petgraph::graph::NodeIndex;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    config::CollectorConfig,
    git::{CommitHash, WorktreeHandle},
    graph::CollectionExecutionGraph,
};

use super::{
    changed_files::ChangedFilesValue,
    utils::{get_previous_commit_value_of_collector, get_value_of_preceeding_node, LookupError},
    BaseCollector, CollectorValue, CollectorValueCastError,
};

#[derive(Deserialize, Debug, Eq, PartialEq, Hash)]
struct CargoTomlPackage {
    name: String,
    version: String,
}

#[derive(Deserialize, Debug)]
struct CargoToml {
    package: Option<CargoTomlPackage>,
}

#[derive(Deserialize, Debug, Eq, PartialEq)]
struct CargoLockPackage(cargo_lock::package::Package);

impl std::hash::Hash for CargoLockPackage {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.checksum.hash(state);
        self.0.dependencies.hash(state);
        self.0.name.hash(state);
        self.0.replace.hash(state);
        self.0.source.hash(state);
        self.0.version.hash(state);
    }
}

#[derive(Debug)]
pub(crate) struct TotalCargoDependencies;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TotalCargoDependenciesValue {
    total_dependencies: u32,
}

#[derive(Error, Debug)]
pub enum TotalCargoDependenciesError {
    #[error("{0}")]
    Lookup(#[from] LookupError),

    #[error("{0}")]
    Cast(#[from] CollectorValueCastError),

    #[error("{0}")]
    IO(#[from] std::io::Error),

    #[error("{0}")]
    Lockfile(#[from] cargo_lock::Error),

    #[error("{0}")]
    TryFromIntError(#[from] std::num::TryFromIntError),

    #[error("{0}")]
    TomlDeserializationError(#[from] toml::de::Error),
}

impl BaseCollector for TotalCargoDependencies {
    type Error = TotalCargoDependenciesError;

    #[tracing::instrument(level = "trace", skip_all)]
    fn collect(
        &self,
        storage: &DashMap<(CollectorConfig, CommitHash), CollectorValue>,
        repo: &mut WorktreeHandle,
        graph: &CollectionExecutionGraph,
        current_node_idx: NodeIndex,
    ) -> Result<CollectorValue, TotalCargoDependenciesError> {
        let changed_files_in_current_commit_value: ChangedFilesValue =
            get_value_of_preceeding_node(
                storage,
                graph,
                current_node_idx,
                |e| e.distance == 0,
                |n| n.collector_config == CollectorConfig::ChangedFiles,
            )?
            .try_into()?;

        let changed_files_in_current_commit: HashSet<String> =
            changed_files_in_current_commit_value.files;

        let modified_cargo_toml_paths: Vec<&String> = changed_files_in_current_commit
            .iter()
            .filter(|relative_path| {
                let path = repo.path.join(relative_path);
                let p = &path;
                p.ends_with("Cargo.toml")
            })
            .collect();

        let modified_cargo_lock_paths: Vec<&String> = changed_files_in_current_commit
            .iter()
            .filter(|relative_path| {
                let path = repo.path.join(relative_path);
                let p = &path;
                p.ends_with("Cargo.lock")
            })
            .collect();

        if modified_cargo_toml_paths.is_empty() && modified_cargo_lock_paths.is_empty() {
            let previous_commit_value =
                get_previous_commit_value_of_collector(storage, graph, current_node_idx);

            if let Some(previous_commit_value) = previous_commit_value {
                return Ok(previous_commit_value);
            }
        }

        let mut crates_in_repo: HashSet<CargoTomlPackage> = HashSet::new();
        for relative_path in modified_cargo_toml_paths {
            let path = repo.path.join(relative_path);
            let p = &path;

            let file = File::open(p)?;
            let mut buf_reader = BufReader::new(file);
            let mut contents = String::new();
            buf_reader.read_to_string(&mut contents)?;

            let cargo_toml: CargoToml = toml::from_str(&contents)?;

            if let Some(package) = cargo_toml.package {
                crates_in_repo.insert(package);
            }
        }

        let mut dependencies: HashSet<CargoLockPackage> = HashSet::new();
        for relative_path in modified_cargo_lock_paths {
            let path = repo.path.join(relative_path);
            let p = &path;

            let lockfile = Lockfile::load(p)?;

            for package in lockfile.packages {
                dependencies.insert(CargoLockPackage(package));
            }
        }

        let dep_count = dependencies
            .iter()
            .filter(|d| {
                !crates_in_repo.contains(&CargoTomlPackage {
                    name: d.0.name.to_string(),
                    version: d.0.version.to_string(),
                })
            })
            .count();

        let value = TotalCargoDependenciesValue {
            total_dependencies: u32::try_from(dep_count)?,
        };

        Ok(value.into())
    }
}
