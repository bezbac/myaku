use std::{collections::HashSet, io::BufWriter};

use anyhow::Result;
use dashmap::DashMap;
use grep::{printer::JSON, regex::RegexMatcher, searcher::SearcherBuilder};
use petgraph::graph::NodeIndex;
use serde::{Deserialize, Serialize};
use walkdir::WalkDir;

use crate::{
    config::CollectorConfig,
    git::{CommitHash, WorktreeHandle},
    graph::CollectionExecutionGraph,
};

use super::{utils::get_previous_commit_value_of_collector, BaseCollector};

#[derive(Clone, Hash, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub(super) struct PartialGrepText {
    pub text: String,
}

#[derive(Clone, Hash, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub(super) struct PartialMatchDataSubmatch {
    pub start: usize,
    pub end: usize,
    #[serde(rename = "match")]
    pub mtch: PartialGrepText,
}

#[derive(Clone, Hash, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub(super) struct PartialMatchData {
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

pub(super) struct PatternOccurences {
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

impl BaseCollector for PatternOccurences {
    fn collect(
        &self,
        storage: &DashMap<(CollectorConfig, CommitHash), String>,
        repo: &mut WorktreeHandle,
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
