use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs::File,
    io::{BufReader, BufWriter, Read},
};

use anyhow::Result;
use cargo_lock::Lockfile;
use grep::{printer::JSON, regex::RegexMatcher, searcher::SearcherBuilder};
use petgraph::graph::{EdgeIndex, NodeIndex};
use serde::{Deserialize, Serialize};
use tokei::{LanguageType, Languages};
use walkdir::WalkDir;

use crate::{
    config::CollectorConfig,
    git::RepositoryHandle,
    graph::{CollectionExecutionGraph, CollectionGraphEdge, CollectionTask},
};

pub trait Collector {
    fn collect(
        &self,
        storage: &HashMap<NodeIndex, String>,
        repo: &RepositoryHandle,
        graph: &CollectionExecutionGraph,
        current_node_idx: &NodeIndex,
    ) -> Result<String>;
}

struct TotalLoc;

impl Collector for TotalLoc {
    fn collect(
        &self,
        _storage: &HashMap<NodeIndex, String>,
        repo: &RepositoryHandle,
        _graph: &CollectionExecutionGraph,
        _current_node_idx: &NodeIndex,
    ) -> Result<String> {
        let mut languages = Languages::new();
        languages.get_statistics(&[&repo.path], &[".git"], &tokei::Config::default());
        let value = languages.total().code;
        let result = serde_json::to_string(&value)?;
        Ok(result)
    }
}

struct Loc;

impl Collector for Loc {
    fn collect(
        &self,
        _storage: &HashMap<NodeIndex, String>,
        repo: &RepositoryHandle,
        _graph: &CollectionExecutionGraph,
        _current_node_idx: &NodeIndex,
    ) -> Result<String> {
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
    fn collect(
        &self,
        _storage: &HashMap<NodeIndex, String>,
        repo: &RepositoryHandle,
        _graph: &CollectionExecutionGraph,
        _current_node_idx: &NodeIndex,
    ) -> Result<String> {
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
    fn collect(
        &self,
        _storage: &HashMap<NodeIndex, String>,
        repo: &RepositoryHandle,
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

#[derive(Clone, Hash, PartialEq, Eq, Debug, Serialize, Deserialize)]
struct PartialGrepText {
    pub text: String,
}

#[derive(Clone, Hash, PartialEq, Eq, Debug, Serialize, Deserialize)]
struct PartialMatchDataSubmatch {
    pub start: usize,
    pub end: usize,
    #[serde(rename = "match")]
    pub mtch: PartialGrepText,
}

#[derive(Clone, Hash, PartialEq, Eq, Debug, Serialize, Deserialize)]
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

struct PatternOccurences {
    pattern: String,
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

fn find_incoming_edges<F: Fn(&CollectionGraphEdge) -> bool>(
    graph: &CollectionExecutionGraph,
    current_node_idx: &NodeIndex,
    predicate: F,
) -> Vec<EdgeIndex> {
    let mut edge_walker = graph
        .graph
        .neighbors_directed(current_node_idx.clone(), petgraph::Direction::Incoming)
        .detach();

    let mut result = Vec::new();

    while let Some(edge_idx) = edge_walker.next_edge(&graph.graph) {
        let edge = &graph.graph[edge_idx];

        if predicate(edge) {
            result.push(edge_idx);
        }
    }

    result
}

fn find_preceding_node<EP: Fn(&CollectionGraphEdge) -> bool, NP: Fn(&CollectionTask) -> bool>(
    graph: &CollectionExecutionGraph,
    current_node_idx: &NodeIndex,
    edge_predicate: EP,
    node_predicate: NP,
) -> Option<NodeIndex> {
    let incoming_edges_matching_predicate =
        find_incoming_edges(graph, current_node_idx, edge_predicate);

    for edge_idx in incoming_edges_matching_predicate {
        let endpoints = graph.graph.edge_endpoints(edge_idx);

        if !endpoints.is_some() {
            continue;
        }

        let source_node_idx = endpoints.unwrap().0;
        let source_node = &graph.graph[source_node_idx];

        if node_predicate(source_node) {
            return Some(source_node_idx);
        }
    }

    None
}

fn get_previous_commit_value_of_collector(
    storage: &HashMap<NodeIndex, String>,
    graph: &CollectionExecutionGraph,
    current_node_idx: &NodeIndex,
) -> Option<String> {
    let current_node = &graph.graph[*current_node_idx];

    let previous_node_index = find_preceding_node(
        graph,
        current_node_idx,
        |e| e.distance == 1,
        |n| n.collector_config == current_node.collector_config,
    );

    if previous_node_index.is_none() {
        return None;
    }

    let previous_node_index = previous_node_index.unwrap();

    storage.get(&previous_node_index).cloned()
}

impl Collector for PatternOccurences {
    fn collect(
        &self,
        storage: &HashMap<NodeIndex, String>,
        repo: &RepositoryHandle,
        graph: &CollectionExecutionGraph,
        current_node_idx: &NodeIndex,
    ) -> Result<String> {
        let files_changed_in_current_commit = repo.get_current_changed_file_paths()?;

        let mut searcher = SearcherBuilder::new().line_number(true).build();
        let matcher = RegexMatcher::new(&self.pattern)?;

        let buffer = BufWriter::new(Vec::new());
        let mut sink = JSON::new(buffer);

        let previous_commit_value =
            get_previous_commit_value_of_collector(storage, graph, current_node_idx);

        if let Some(previous_commit_value) = previous_commit_value {
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

            let previous_commit_matches: HashSet<PartialMatchData> =
                serde_json::from_str(&previous_commit_value)?;

            let filtered_cached_matches: HashSet<PartialMatchData> = previous_commit_matches
                .iter()
                .filter(|m| !files_changed_in_current_commit.contains(&m.path.text))
                .cloned()
                .collect();

            let combined_matches: HashSet<PartialMatchData> =
                filtered_cached_matches.union(&matches).cloned().collect();

            let result = serde_json::to_string(&combined_matches)?;

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

            let result = serde_json::to_string(&matches)?;

            Ok(result)
        }
    }
}

struct TotalPatternOccurences {
    pattern: String,
}

impl Collector for TotalPatternOccurences {
    fn collect(
        &self,
        storage: &HashMap<NodeIndex, String>,
        _repo: &RepositoryHandle,
        graph: &CollectionExecutionGraph,
        current_node_idx: &NodeIndex,
    ) -> Result<String> {
        let pattern_occurences_task_idx = find_preceding_node(
            graph,
            current_node_idx,
            |e| e.distance == 0,
            |n| {
                n.collector_config
                    == CollectorConfig::PatternOccurences {
                        pattern: self.pattern.clone(),
                    }
            },
        )
        .unwrap_or_else(|| {
            panic!(
                "Could not find required dependency task for node {:?}",
                current_node_idx
            )
        });

        let pattern_occurences_value =
            storage.get(&pattern_occurences_task_idx).ok_or_else(|| {
                anyhow::anyhow!(
                    "Could not read required value from storage for node {:?}",
                    pattern_occurences_task_idx
                )
            })?;

        let matches: HashSet<PartialMatchData> = serde_json::from_str(&pattern_occurences_value)?;

        let total_matches = matches.len();

        let result = serde_json::to_string(&total_matches)?;

        return Ok(result);
    }
}

impl From<&CollectorConfig> for Box<dyn Collector> {
    fn from(value: &CollectorConfig) -> Self {
        match value {
            CollectorConfig::Loc => Box::new(Loc {}),
            CollectorConfig::TotalLoc => Box::new(TotalLoc {}),
            CollectorConfig::TotalDiffStat => Box::new(TotalDiffStat {}),
            CollectorConfig::TotalCargoDeps => Box::new(TotalCargoDependencies {}),
            CollectorConfig::PatternOccurences { pattern } => Box::new(PatternOccurences {
                pattern: pattern.clone(),
            }),
            CollectorConfig::TotalPatternOccurences { pattern } => {
                Box::new(TotalPatternOccurences {
                    pattern: pattern.clone(),
                })
            }
        }
    }
}
