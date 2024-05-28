use std::{
    collections::{BTreeMap, HashSet},
    fs::File,
    io::{BufReader, BufWriter, Read},
    sync::RwLock,
};

use anyhow::Result;
use cargo_lock::Lockfile;
use grep::{printer::JSON, regex::RegexMatcher, searcher::SearcherBuilder};
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
        let (files_changed, insertions, deletions) = repo.get_current_total_diff_stat().unwrap();

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

#[derive(Clone, Hash, PartialEq, Eq, Debug, Deserialize)]
struct PartialGrepText {
    pub text: String,
}

#[derive(Clone, Hash, PartialEq, Eq, Debug, Deserialize)]
struct PartialMatchDataSubmatch {
    pub start: usize,
    pub end: usize,
    #[serde(rename = "match")]
    pub mtch: PartialGrepText,
}

#[derive(Clone, Hash, PartialEq, Eq, Debug, Deserialize)]
struct PartialMatchData {
    pub path: PartialGrepText,
    pub line_number: usize,
    pub absolute_offset: usize,
    pub submatches: Vec<PartialMatchDataSubmatch>,
}

#[derive(Hash, PartialEq, Eq, Debug, Deserialize)]
#[serde(tag = "type")]
enum PartialGrepJSONLine {
    #[serde(rename = "match")]
    Match { data: PartialMatchData },
}

struct TotalPatternOccurences {
    pattern: String,

    cache: RwLock<Option<HashSet<PartialMatchData>>>,
}

fn get_matches_from_grep_output(output: &str) -> HashSet<PartialMatchData> {
    output
        .lines()
        .filter_map(|l| serde_json::from_str::<PartialGrepJSONLine>(l).ok())
        .map(|l| match l {
            PartialGrepJSONLine::Match { data } => data,
        })
        .collect()
}

fn get_matches_from_sink(sink: JSON<BufWriter<Vec<u8>>>) -> Result<HashSet<PartialMatchData>> {
    let bytes = sink.into_inner().into_inner()?;
    let ripgrep_output = String::from_utf8(bytes)?;

    let matches = get_matches_from_grep_output(&ripgrep_output);

    Ok(matches)
}

// This collector assumes it's run per commit and in order. If it isn't, it will return false data!
impl Collector for TotalPatternOccurences {
    fn collect(&self, repo: &RepositoryHandle) -> Result<String> {
        let files_changed_in_current_commit = repo.get_current_changed_file_paths()?;

        let mut searcher = SearcherBuilder::new().line_number(true).build();
        let matcher = RegexMatcher::new(&self.pattern)?;

        let buffer = BufWriter::new(Vec::new());
        let mut sink = JSON::new(buffer);

        let cache = self.cache.read().unwrap().clone();

        if let Some(cache) = cache {
            for changed_file_relative_path in &files_changed_in_current_commit {
                let changed_file_absolute_path = repo.path.join(&changed_file_relative_path);

                if !changed_file_absolute_path.exists() {
                    // File was removed in the current commit
                    // TODO: Already get this information from git to be more certain
                    continue;
                }

                let sink = sink.sink_with_path(&matcher, &changed_file_relative_path);
                searcher.search_path(&matcher, &changed_file_absolute_path, sink)?;
            }

            let matches = get_matches_from_sink(sink)?;

            let filtered_cached_matches: HashSet<PartialMatchData> = cache
                .iter()
                .filter(|m| !files_changed_in_current_commit.contains(&m.path.text))
                .cloned()
                .collect();

            let combined_matches: HashSet<PartialMatchData> =
                filtered_cached_matches.union(&matches).cloned().collect();

            let total_match_count = combined_matches.len();

            let mut cache_lock = self.cache.write().unwrap();
            *cache_lock = Some(combined_matches);

            let result = serde_json::to_string(&total_match_count)?;

            Ok(result)
        } else {
            let root_path = &repo.path.canonicalize()?;

            for entry in WalkDir::new(&root_path).into_iter().filter_entry(|e| {
                // Skip .git directory
                let is_dot_git_dir = e
                    .file_name()
                    .to_str()
                    .map(|s| s.starts_with(".git"))
                    .unwrap_or(false);

                !is_dot_git_dir
            }) {
                let entry = entry?;

                if !entry.file_type().is_file() {
                    continue;
                }

                let path = entry.path();
                let path_relative_to_root = path.canonicalize()?;
                let path_relative_to_root = path_relative_to_root.strip_prefix(root_path)?;

                let mut sink = sink.sink_with_path(&matcher, path_relative_to_root);
                searcher.search_path(&matcher, entry.path(), &mut sink)?;
            }

            let matches = get_matches_from_sink(sink)?;

            let total_match_count = matches.len();

            // Update the cache
            let mut cache_lock = self.cache.write().unwrap();
            *cache_lock = Some(matches);

            let result = serde_json::to_string(&total_match_count)?;

            Ok(result)
        }
    }
}

impl From<CollectorConfig> for Box<dyn Collector> {
    fn from(value: CollectorConfig) -> Self {
        match value {
            CollectorConfig::Loc => Box::new(Loc {}),
            CollectorConfig::TotalLoc => Box::new(TotalLoc {}),
            CollectorConfig::TotalDiffStat => Box::new(TotalDiffStat {}),
            CollectorConfig::TotalCargoDeps => Box::new(TotalCargoDependencies {}),
            CollectorConfig::TotalPatternOccurences { pattern } => {
                Box::new(TotalPatternOccurences {
                    pattern: pattern.clone(),
                    cache: RwLock::new(None),
                })
            }
        }
    }
}
