use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicI32, Ordering},
        mpsc, Mutex,
    },
};

use dashmap::DashMap;
use marzano_core::{
    api::{is_match, MatchResult},
    pattern_compiler::CompiledPatternBuilder,
};
use marzano_language::target_language::expand_paths;
use marzano_language::target_language::TargetLanguage;
use marzano_util::{
    cache::NullCache,
    rich_path::{RichFile, RichPath},
    runtime::ExecutionContext,
};
use petgraph::graph::NodeIndex;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::debug;

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
    Ignore(#[from] ignore::Error),
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
        let paths: Vec<PathBuf> = vec![repo.path.clone()];

        dbg!("Compliting pattern");

        // TODO: Handle compilation outside of the collector

        let compiler = CompiledPatternBuilder::start_empty(&self.pattern, language)
            .map_err(|err| GritQLPatternOccurencesError::Marzano(err))?;

        let compilation_result = compiler
            .compile(None, None, false)
            .map_err(|err| GritQLPatternOccurencesError::Marzano(err))?;

        // TODO: Handle compilation errors

        let compiled = compilation_result.problem;

        dbg!(&compiled);

        let (file_paths_tx, file_paths_rx) = mpsc::channel();

        let file_walker = expand_paths(&paths, Some(&[(&compiled.language).into()]))
            .map_err(|err| GritQLPatternOccurencesError::Marzano(err))?;

        // TODO: Use walkdir instead of ignore
        for file in file_walker {
            let file = file?;

            if file.file_type().unwrap().is_dir() {
                continue;
            }

            if !paths.contains(&file.path().to_path_buf()) {
                let ext = file
                    .path()
                    .extension()
                    .unwrap_or_default()
                    .to_str()
                    .unwrap_or_default();
                if !&compiled.language.match_extension(ext) {
                    // only skip the file if it was discovered by the walker
                    // don't skip if it was explicitly passed in as a path
                    // https://github.com/getgrit/gritql/issues/485
                    continue;
                }
            }

            file_paths_tx.send(file.path().to_path_buf()).unwrap();
        }

        let found_paths = file_paths_rx.iter().collect::<Vec<_>>();

        let found_paths_count = found_paths.len();

        let (tx, rx) = mpsc::channel::<Vec<MatchResult>>();

        let matched = AtomicI32::new(0);
        let processed = AtomicI32::new(0);
        // let matches: Mutex<Vec<MatchResult>> = Mutex::new(Vec::new());
        let cache = NullCache::new();
        let context = ExecutionContext::new();

        let files: Vec<RichPath> = found_paths
            .into_iter()
            .map(|path| RichPath::new(path, None))
            .collect();

        dbg!(&files);

        let res = compiled.execute_paths(files.iter().collect(), &context);

        dbg!(res);

        // rayon::scope(|s| {
        //     s.spawn(|_| {
        //         for message in rx {
        //             for r in message {
        //                 if is_match(&r) {
        //                     let count = r.get_ranges().map(|ranges| ranges.len()).unwrap_or(0);
        //                     matched.fetch_add(count.max(1) as i32, Ordering::SeqCst);

        //                     dbg!(r);
        //                 }

        //                 if let MatchResult::DoneFile(_) = r {
        //                     processed.fetch_add(1, Ordering::SeqCst);
        //                 }

        //                 // TODO: Handle other match result types

        //                 // TODO: Don't use unwrap here
        //                 // let mut matches = matches.lock().unwrap();
        //                 // matches.push(r);
        //             }
        //         }
        //     });

        //     let task_span = tracing::info_span!("apply_file_one_streaming").entered();
        //     task_span.in_scope(|| {
        //         compiled.execute_paths_streaming(found_paths, &context, tx, &cache);

        //         loop {
        //             if processed.load(Ordering::SeqCst) >= found_paths_count.try_into().unwrap() {
        //                 break;
        //             }
        //         }
        //     });
        // });

        let value = GritQLPatternOccurencesValue {};

        Ok(value.into())
    }
}

// Pattern application
// https://github.com/getgrit/gritql/blob/dce415566e7e82140e421881257c9c23a8ad8c8a/crates/cli/src/commands/apply_pattern.rs#L553-L583
