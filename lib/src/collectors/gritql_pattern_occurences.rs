use dashmap::DashMap;
use marzano_core::pattern_compiler::CompiledPatternBuilder;
use marzano_language::target_language::TargetLanguage;
use marzano_util::{rich_path::RichFile, runtime::ExecutionContext};
use petgraph::graph::NodeIndex;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use walkdir::WalkDir;

use crate::{
    config::{CollectorConfig, GritQLLanguage},
    git::{CommitHash, WorktreeHandle},
    graph::CollectionExecutionGraph,
};

use super::{BaseCollector, CollectorValue};

impl From<&GritQLLanguage> for TargetLanguage {
    fn from(language: &GritQLLanguage) -> Self {
        match language {
            GritQLLanguage::JavaScript => TargetLanguage::from_string("js", None).unwrap(),
            GritQLLanguage::TypeScript => TargetLanguage::from_string("ts", None).unwrap(),
            GritQLLanguage::Rust => TargetLanguage::from_string("rs", None).unwrap(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct GritQLPatternOccurences {
    pub pattern: String,
    pub language: GritQLLanguage,
    // TODO: Support globs
    // pub files: Option<Vec<Glob>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GritQLPatternOccurencesValue {}

#[derive(Error, Debug)]
pub enum GritQLPatternOccurencesError {
    #[error("{0}")]
    Marzano(anyhow::Error),

    #[error("{0}")]
    IO(#[from] std::io::Error),

    #[error("{0}")]
    Walkdir(#[from] walkdir::Error),
}

impl BaseCollector for GritQLPatternOccurences {
    type Error = GritQLPatternOccurencesError;

    #[tracing::instrument(level = "trace", skip_all)]
    fn collect(
        &self,
        _storage: &DashMap<(CollectorConfig, CommitHash), CollectorValue>,
        repo: &mut WorktreeHandle,
        _graph: &CollectionExecutionGraph,
        _current_node_idx: NodeIndex,
    ) -> Result<CollectorValue, GritQLPatternOccurencesError> {
        let language: TargetLanguage = (&self.language).into();

        dbg!("Compliting pattern");

        // TODO: Handle compilation outside of the collector

        let compiler = CompiledPatternBuilder::start_empty(&self.pattern, language)
            .map_err(|err| GritQLPatternOccurencesError::Marzano(err))?;

        let compilation_result = compiler
            .compile(None, None, false)
            .map_err(|err| GritQLPatternOccurencesError::Marzano(err))?;

        // TODO: Handle compilation errors

        let compiled = compilation_result.problem;

        dbg!("Walking paths");

        let root_path = &repo.path.canonicalize()?;

        let mut found_paths = Vec::new();

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

            let ext = entry
                .path()
                .extension()
                .unwrap_or_default()
                .to_str()
                .unwrap_or_default();

            if !&compiled.language.match_extension(ext) {
                continue;
            }

            found_paths.push(entry.into_path());
        }

        dbg!(&found_paths);

        let context = ExecutionContext::new();

        for file in &found_paths {
            let file_name = file
                .file_name()
                .unwrap()
                .to_str()
                .unwrap_or_default()
                .to_string();

            let content = std::fs::read_to_string(file)?;

            let rich_file = RichFile::new(file_name, content);

            let matches = compiled.execute_file(&rich_file, &context);

            dbg!(matches);
        }

        let value = GritQLPatternOccurencesValue {};

        Ok(value.into())
    }
}

// Pattern application
// https://github.com/getgrit/gritql/blob/dce415566e7e82140e421881257c9c23a8ad8c8a/crates/cli/src/commands/apply_pattern.rs#L553-L583
