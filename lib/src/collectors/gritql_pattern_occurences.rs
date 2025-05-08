use std::{
    collections::{BTreeMap, HashMap},
    path::PathBuf,
    sync::mpsc,
};

use dashmap::DashMap;
use globset::Glob;
use marzano_core::{api::MatchResult, pattern_compiler::CompiledPatternBuilder};
use marzano_language::target_language::expand_paths;
use marzano_language::target_language::TargetLanguage;
use petgraph::graph::NodeIndex;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    config::CollectorConfig,
    git::{CommitHash, WorktreeHandle},
    graph::CollectionExecutionGraph,
};

use super::{BaseCollector, CollectorValue};

#[derive(Debug)]
pub(crate) struct GritQLPatternOccurences {
    pub pattern: String,
    pub files: Option<Vec<Glob>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GritQLPatternOccurencesValue {}

#[derive(Error, Debug)]
pub enum GritQLPatternOccurencesError {}

impl BaseCollector for GritQLPatternOccurences {
    type Error = GritQLPatternOccurencesError;

    #[tracing::instrument(level = "trace", skip_all)]
    fn collect(
        &self,
        storage: &DashMap<(CollectorConfig, CommitHash), CollectorValue>,
        repo: &mut WorktreeHandle,
        graph: &CollectionExecutionGraph,
        current_node_idx: NodeIndex,
    ) -> Result<CollectorValue, GritQLPatternOccurencesError> {
        let pattern_body: String = "".to_string();
        let pattern_libs: BTreeMap<String, String> = BTreeMap::new();
        let paths: Vec<PathBuf> = vec![];

        let language = TargetLanguage::from_string("js", None).unwrap();

        let compiler = CompiledPatternBuilder::start_empty("`console.log($arg)`", language)?;

        let compilation_result = compiler.compile(None, None, false)?;

        // TODO: Handle compilation errors

        let compiled = compilation_result.problem;

        let (file_paths_tx, file_paths_rx) = mpsc::channel();

        let file_walker = expand_paths(&paths, Some(&[(&compiled.language).into()]))?;

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

        drop(file_paths_tx);

        let found_paths = file_paths_rx.iter().collect::<Vec<_>>();

        let (tx, rx) = mpsc::channel::<Vec<MatchResult>>();

        rayon::scope(|s| {
            s.spawn(move |_| {
                let mut parse_errors: HashMap<String, usize> = HashMap::new();
                for message in rx {
                    let user_decision = emitter.handle_results(
                        message,
                        details,
                        arg.dry_run,
                        arg.format,
                        &mut interactive,
                        pg,
                        Some(processed),
                        Some(&mut parse_errors),
                        compiled_language,
                    );

                    if !user_decision {
                        should_continue.store(false, Ordering::SeqCst);
                        break;
                    }

                    if !should_continue.load(Ordering::SeqCst) {
                        break;
                    }
                }
            });

            let task_span = tracing::info_span!("apply_file_one_streaming").entered();
            task_span.in_scope(|| {
                compiled.execute_paths_streaming(found_paths, context, tx, cache_ref);

                loop {
                    if processed.load(Ordering::SeqCst) >= found_count.try_into().unwrap()
                        || !should_continue.load(Ordering::SeqCst)
                    {
                        break;
                    }
                }
            });
        });

        Ok(CollectorValue::default())
    }
}

// Pattern application
// https://github.com/getgrit/gritql/blob/dce415566e7e82140e421881257c9c23a8ad8c8a/crates/cli/src/commands/apply_pattern.rs#L553-L583
