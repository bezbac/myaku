use std::{
    collections::HashSet,
    fs::File,
    io::{BufReader, Read},
};

use anyhow::Result;
use cargo_lock::Lockfile;
use dashmap::DashMap;
use petgraph::graph::NodeIndex;
use serde::Deserialize;
use walkdir::WalkDir;

use crate::{
    config::CollectorConfig,
    git::{CommitHash, WorktreeHandle},
    graph::CollectionExecutionGraph,
};

use super::Collector;

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

pub(super) struct TotalCargoDependencies;

impl Collector for TotalCargoDependencies {
    fn collect(
        &self,
        _storage: &DashMap<(CollectorConfig, CommitHash), String>,
        repo: &mut WorktreeHandle,
        _graph: &CollectionExecutionGraph,
        _current_node_idx: &NodeIndex,
    ) -> Result<String> {
        let mut crates_in_repo: HashSet<CargoTomlPackage> = HashSet::new();
        let mut dependencies: HashSet<CargoLockPackage> = HashSet::new();

        for entry in WalkDir::new(&repo.path).into_iter() {
            let entry = entry?;

            if entry.path().components().any(|f| f.as_os_str() == ".git") {
                continue;
            }

            if let Some(path) = entry.path().to_str() {
                if path.ends_with("Cargo.toml") {
                    let file = File::open(path)?;
                    let mut buf_reader = BufReader::new(file);
                    let mut contents = String::new();
                    buf_reader.read_to_string(&mut contents)?;

                    let cargo_toml: CargoToml = toml::from_str(&contents)?;

                    if let Some(package) = cargo_toml.package {
                        crates_in_repo.insert(package);
                    }
                }

                if path.ends_with("Cargo.lock") {
                    let lockfile = Lockfile::load(path)?;

                    for package in lockfile.packages {
                        dependencies.insert(CargoLockPackage(package));
                    }
                }
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

        let result = serde_json::to_string(&dep_count)?;

        Ok(result)
    }
}
