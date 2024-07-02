use std::{collections::HashSet, io::BufWriter};

use anyhow::Result;
use dashmap::DashMap;
use grep::{printer::JSON, regex::RegexMatcher, searcher::SearcherBuilder};
use petgraph::graph::NodeIndex;
use serde::{Deserialize, Serialize};
use tracing::debug;
use walkdir::WalkDir;

use crate::{
    config::CollectorConfig,
    git::{CommitHash, WorktreeHandle},
    graph::CollectionExecutionGraph,
};

use super::{
    changed_files::ChangedFilesValue,
    utils::{get_previous_commit_value_of_collector, get_value_of_preceeding_node},
    BaseCollector, CollectorValue,
};

#[derive(Clone, Hash, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct PartialGrepText {
    pub text: String,
}

#[derive(Clone, Hash, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct PartialMatchDataSubmatch {
    pub start: usize,
    pub end: usize,
    #[serde(rename = "match")]
    pub mtch: PartialGrepText,
}

#[derive(Clone, Hash, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct PartialMatchData {
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

#[derive(Debug)]
pub struct PatternOccurences {
    pub pattern: String,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PatternOccurencesValue {
    pub matches: HashSet<PartialMatchData>,
}

impl BaseCollector for PatternOccurences {
    #[tracing::instrument(level = "trace", skip_all)]
    fn collect(
        &self,
        storage: &DashMap<(CollectorConfig, CommitHash), CollectorValue>,
        repo: &mut WorktreeHandle,
        graph: &CollectionExecutionGraph,
        current_node_idx: NodeIndex,
    ) -> Result<CollectorValue> {
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

        let mut searcher = SearcherBuilder::new().line_number(true).build();
        let matcher = RegexMatcher::new(&self.pattern)?;

        let buffer = BufWriter::new(Vec::new());
        let mut sink = JSON::new(buffer);

        let previous_commit_value =
            get_previous_commit_value_of_collector(storage, graph, current_node_idx);

        if let Some(previous_commit_value) = previous_commit_value {
            debug!("found value from previous commit, only searching changed files");

            for changed_file_relative_path in &changed_files_in_current_commit {
                let changed_file_absolute_path = repo.path.join(changed_file_relative_path);

                if !changed_file_absolute_path.exists() {
                    // File was removed in the current commit
                    // TODO: Already get this information from git to be more certain
                    continue;
                }

                debug!("searching file: {:?}", changed_file_relative_path);

                let sink = sink.sink_with_path(&matcher, &changed_file_relative_path);
                searcher.search_path(&matcher, &changed_file_absolute_path, sink)?;
            }

            let matches = get_matches_from_sink(sink)?;

            let previous_commit_value: PatternOccurencesValue = previous_commit_value.try_into()?;

            let previous_commit_matches: HashSet<PartialMatchData> = previous_commit_value.matches;

            let filtered_cached_matches: HashSet<PartialMatchData> = previous_commit_matches
                .iter()
                .filter(|m| !changed_files_in_current_commit.contains(&m.path.text))
                .cloned()
                .collect();

            let combined_matches: HashSet<PartialMatchData> =
                filtered_cached_matches.union(&matches).cloned().collect();

            let value = PatternOccurencesValue {
                matches: combined_matches,
            };

            Ok(value.into())
        } else {
            debug!("did not find value from previous commit, searching all files");

            let root_path = &repo.path.canonicalize()?;

            for entry in WalkDir::new(root_path).into_iter().filter_entry(|e| {
                // Skip .git directory
                let is_dot_git_dir = e
                    .file_name()
                    .to_str()
                    .is_some_and(|s| s.starts_with(".git"));

                !is_dot_git_dir
            }) {
                let entry = entry?;

                if !entry.file_type().is_file() {
                    continue;
                }

                let path = entry.path();
                let path_relative_to_root = path.canonicalize()?;
                let path_relative_to_root = path_relative_to_root.strip_prefix(root_path)?;

                debug!("searching file: {:?}", path_relative_to_root);

                let mut sink = sink.sink_with_path(&matcher, path_relative_to_root);
                searcher.search_path(&matcher, entry.path(), &mut sink)?;
            }

            let matches = get_matches_from_sink(sink)?;

            let value = PatternOccurencesValue { matches };

            Ok(value.into())
        }
    }
}
