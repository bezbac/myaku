use std::{
    collections::{BTreeMap, HashSet},
    fs::File,
    io::{BufReader, Read},
};

use anyhow::Result;
use cargo_lock::Lockfile;
use serde::Deserialize;
use tokei::{LanguageType, Languages};
use walkdir::WalkDir;

use crate::{config::CollectorConfig, git::RepositoryHandle};

pub trait Collector {
    fn collect(&self, repo: &RepositoryHandle) -> Result<String>;
}

struct TotalLoc;

impl Collector for TotalLoc {
    fn collect(&self, repo: &RepositoryHandle) -> Result<String> {
        let mut languages = Languages::new();
        languages.get_statistics(&[&repo.path], &[".git"], &tokei::Config::default());
        let value = languages.total().code;
        let result = serde_json::to_string(&value)?;
        Ok(result)
    }
}

struct Loc;

impl Collector for Loc {
    fn collect(&self, repo: &RepositoryHandle) -> Result<String> {
        let mut languages = Languages::new();
        languages.get_statistics(&[&repo.path], &[".git"], &tokei::Config::default());
        let value: BTreeMap<&LanguageType, usize> = languages
            .iter()
            .map(|(lang, info)| (lang, info.code))
            .filter(|(_, value)| *value > 0)
            .collect();
        let result = serde_json::to_string(&value)?;
        Ok(result)
    }
}

struct TotalDiffStat;

impl Collector for TotalDiffStat {
    fn collect(&self, repo: &RepositoryHandle) -> Result<String> {
        let (files_changed, insertions, deletions) =
            repo.get_current_total_diff_stat().unwrap_or((0, 0, 0));

        let result = serde_json::to_string(&(files_changed, insertions, deletions))?;
        Ok(result)
    }
}

#[derive(Deserialize, Debug, Eq, PartialEq, Hash)]
struct CargoTomlPackage {
    name: String,
    version: String,
}

#[derive(Deserialize, Debug)]
struct CargoToml {
    package: CargoTomlPackage,
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

struct TotalCargoDependencies;

impl Collector for TotalCargoDependencies {
    fn collect(&self, repo: &RepositoryHandle) -> Result<String> {
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

                    crates_in_repo.insert(cargo_toml.package);
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

impl Collector for CollectorConfig {
    fn collect(&self, repo: &RepositoryHandle) -> Result<String> {
        match self {
            CollectorConfig::Loc => Loc {}.collect(repo),
            CollectorConfig::TotalLoc => TotalLoc {}.collect(repo),
            CollectorConfig::TotalDiffStat => TotalDiffStat {}.collect(repo),
            CollectorConfig::TotalCargoDeps => TotalCargoDependencies {}.collect(repo),
        }
    }
}
