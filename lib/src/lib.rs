use std::collections::HashMap;
use std::fs::{self};
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Ok, Result};
use collectors::{Collector, CollectorValue};
use console::Term;
use dashmap::DashMap;
use git::CommitInfo;
use graph::CollectionExecutionGraph;
use indicatif::{ProgressBar, ProgressStyle};
use log::debug;
use nanoid::nanoid;
use object_pool::Pool;
use petgraph::graph::NodeIndex;
use petgraph::visit::Walker;
#[cfg(feature = "rayon")]
use rayon::prelude::*;

use crate::git::CommitHash;
use crate::git::RepositoryHandle;
use crate::git::{clone_repository, CloneProgress};
use crate::graph::build_collection_execution_graph;

mod cache;
mod collectors;
mod config;
mod git;
mod graph;
mod output;

pub use cache::{Cache, FileCache};
pub use config::{CollectorConfig, GitRepository, MetricConfig};
pub use output::{JsonOutput, Output, ParquetOutput};

pub struct ReadyForClone {}

pub struct ReadyForFetch {
    repo: RepositoryHandle,
}

pub struct IdleWithoutCommits {
    repo: RepositoryHandle,
}

pub struct IdleWithCommits {
    repo: RepositoryHandle,
    commits: Vec<CommitInfo>,
    storage: DashMap<(CollectorConfig, CommitHash), CollectorValue>,
}

pub struct ReadyForCollection {
    repo: RepositoryHandle,
    collection_execution_graph: CollectionExecutionGraph,
    storage: DashMap<(CollectorConfig, CommitHash), CollectorValue>,
}

pub struct PostCollection {
    collection_execution_graph: CollectionExecutionGraph,
    storage: DashMap<(CollectorConfig, CommitHash), CollectorValue>,
}

pub enum CollectionProcessState {
    /// The collection process has been created but nothing has been executed yet
    Initial,

    /// The repository does not exist inside the specified path but we can clone it into it
    ReadyForClone(ReadyForClone),

    /// The repository does exist in the specified path and can be refreshed
    ReadyForFetch(ReadyForFetch),

    /// Ready to work with the repository
    IdleWithoutCommits(IdleWithoutCommits),

    /// Ready to build the execution graph
    IdleWithCommits(IdleWithCommits),

    /// The execution graph has been built and can be run
    ReadyForCollection(ReadyForCollection),

    /// The collection has been executed and the results can be accessed
    PostCollection(PostCollection),
}

pub struct CollectionProcess<'t> {
    pub state: CollectionProcessState,

    pub term: &'t Term,

    pub reference: GitRepository,
    pub repository_path: PathBuf,

    pub metrics: HashMap<String, MetricConfig>,

    pub worktree_path: PathBuf,

    pub output: Box<dyn Output>,
    pub cache: Box<dyn Cache>,

    pub disable_cache: bool,
}

impl<'t> CollectionProcess<'t> {
    pub fn execute_initial(mut self) -> Result<CollectionProcess<'t>> {
        return if let CollectionProcessState::Initial = self.state {
            let reference_dir = &self.repository_path;

            fs::create_dir_all(&reference_dir)?;

            match RepositoryHandle::open(&reference_dir) {
                Result::Ok(repo) => {
                    writeln!(
                        self.term,
                        "Repository already exists in reference directory"
                    )?;

                    let remote_url = repo.remote_url()?;
                    if remote_url != self.reference.url {
                        return Err(anyhow::anyhow!("Repository URL in reference directory does not match the one in the config file"));
                    }

                    self.state = CollectionProcessState::ReadyForFetch(ReadyForFetch { repo })
                }
                Err(_) => {
                    // TODO: Check specific error
                    self.state = CollectionProcessState::ReadyForClone(ReadyForClone {})
                }
            };

            Ok(self)
        } else {
            Err(anyhow::anyhow!("Invalid state"))
        };
    }

    pub fn execute_fetch(mut self) -> Result<CollectionProcess<'t>> {
        return if let CollectionProcessState::ReadyForFetch(ReadyForFetch { repo }) = self.state {
            writeln!(self.term, "Refreshing repository")?;

            repo.fetch()?;

            self.term.clear_last_lines(1)?;
            writeln!(self.term, "Refreshed repository successfully")?;

            self.state = CollectionProcessState::IdleWithoutCommits(IdleWithoutCommits { repo });

            Ok(self)
        } else {
            Err(anyhow::anyhow!("Invalid state"))
        };
    }

    pub fn execute_clone(mut self) -> Result<CollectionProcess<'t>> {
        return if let CollectionProcessState::ReadyForClone(ReadyForClone {}) = self.state {
            writeln!(
                self.term,
                "Cloning repository into {}",
                &self.repository_path.display()
            )?;

            let pb = ProgressBar::new(1000);
            let style =
                ProgressStyle::with_template(" {spinner} [{elapsed_precise}] [{bar:40}] {msg}")
                    .unwrap()
                    .progress_chars("#>-");
            pb.set_style(style);
            pb.enable_steady_tick(Duration::from_millis(100));

            pb.set_message("Initializing");

            let repo = clone_repository(&self.reference.url, &self.repository_path, |progress| {
                let bar = &pb;

                match progress {
                    CloneProgress::EnumeratingObjects => {
                        bar.set_message("Enumerating objects");
                    }
                    CloneProgress::CountingObjects { finished, total } => {
                        bar.set_message(format!("Counting objects [{}, {}]", finished, total));
                        bar.set_length(*total as u64);
                        bar.set_position(*finished as u64);
                    }
                    CloneProgress::CompressingObjects { finished, total } => {
                        bar.set_message(format!("Compressing objects [{}, {}]", finished, total));
                        bar.set_length(*total as u64);
                        bar.set_position(*finished as u64);
                    }
                    CloneProgress::ReceivingObjects { finished, total } => {
                        bar.set_message(format!("Receiving objects [{}, {}]", finished, total));
                        bar.set_length(*total as u64);
                        bar.set_position(*finished as u64);
                    }
                    CloneProgress::ResolvingDeltas { finished, total } => {
                        bar.set_message(format!("Resolving deltas [{}, {}]", finished, total));
                        bar.set_length(*total as u64);
                        bar.set_position(*finished as u64);
                    }
                }
            })?;

            pb.finish_and_clear();

            self.term.clear_last_lines(1)?;
            writeln!(
                self.term,
                "Successfully cloned repository into {}",
                &self.repository_path.display()
            )?;

            self.state = CollectionProcessState::IdleWithoutCommits(IdleWithoutCommits { repo });

            Ok(self)
        } else {
            Err(anyhow::anyhow!("Invalid state"))
        };
    }

    pub fn execute_collect_commits(mut self) -> Result<CollectionProcess<'t>> {
        if let CollectionProcessState::IdleWithoutCommits(IdleWithoutCommits { repo }) = self.state
        {
            let branch = match &self.reference.branch {
                Some(branch) => branch.clone(),
                None => repo.find_main_branch()?,
            };

            repo.reset_hard(&format!("origin/{}", branch))?;

            writeln!(self.term, "Collecting commit information")?;
            let commits = repo.get_all_commits()?;
            self.output.set_commits(&commits)?;
            self.term.clear_last_lines(1)?;
            writeln!(self.term, "Collected commit information")?;

            self.state = CollectionProcessState::IdleWithCommits(IdleWithCommits {
                repo,
                commits,
                storage: DashMap::new(),
            })
        } else {
            return Err(anyhow::anyhow!("Invalid state"));
        };

        Ok(self)
    }

    pub fn execute_collect_tags(mut self) -> Result<CollectionProcess<'t>> {
        if let CollectionProcessState::IdleWithCommits(IdleWithCommits {
            repo,
            storage,
            commits: _commits,
        }) = &self.state
        {
            let branch = match &self.reference.branch {
                Some(branch) => branch.clone(),
                None => repo.find_main_branch()?,
            };

            repo.reset_hard(&format!("origin/{}", branch))?;

            writeln!(self.term, "Collecting tag information")?;
            let tags = repo.get_all_commit_tags()?;
            self.output.set_commit_tags(&tags)?;
            self.term.clear_last_lines(1)?;
            writeln!(self.term, "Collected tag information")?;
        } else {
            return Err(anyhow::anyhow!("Invalid state"));
        };

        Ok(self)
    }

    pub fn execute_prepare_for_collection(mut self) -> Result<CollectionProcess<'t>> {
        return if let CollectionProcessState::IdleWithCommits(IdleWithCommits {
            repo,
            commits,
            storage,
        }) = self.state
        {
            writeln!(self.term, "Building execution graph")?;

            if !self.disable_cache {
                self.output.load()?;

                // Fill storage from previous output
                for commit in &commits {
                    for (metric_name, metric_config) in &self.metrics {
                        if let Some(value) = self.output.get_metric(&metric_name, &commit.id)? {
                            storage.insert(
                                (metric_config.collector.clone(), commit.id.clone()),
                                value,
                            );
                        }
                    }
                }
            }

            let collection_execution_graph =
                build_collection_execution_graph(&self.metrics, &commits)?;

            if !self.disable_cache {
                // Fill storage from cache
                for nx in collection_execution_graph.graph.node_indices() {
                    let task = &collection_execution_graph.graph[nx];

                    if let Some(value) = self
                        .cache
                        .lookup(&task.collector_config, &task.commit_hash)?
                    {
                        storage.insert(
                            (task.collector_config.clone(), task.commit_hash.clone()),
                            value,
                        );
                    }
                }
            }

            self.state = CollectionProcessState::ReadyForCollection(ReadyForCollection {
                repo,
                collection_execution_graph,
                storage,
            });

            Ok(self)
        } else {
            Err(anyhow::anyhow!("Invalid state"))
        };
    }

    pub fn execute_collection(mut self) -> Result<CollectionProcess<'t>> {
        if let CollectionProcessState::ReadyForCollection(ReadyForCollection {
            repo,
            collection_execution_graph,
            storage,
        }) = self.state
        {
            writeln!(self.term, "Collecting data points")?;

            let pb = ProgressBar::new(1);
            let style =
                ProgressStyle::with_template(" {spinner} [{elapsed_precise}] [{bar:40}] {msg}")
                    .unwrap()
                    .progress_chars("#>-");
            pb.set_style(style);
            pb.enable_steady_tick(Duration::from_millis(100));

            pb.set_length(collection_execution_graph.graph.node_count().try_into()?);

            let new_metric_count = Arc::new(Mutex::new(0));
            let reused_metric_count = Arc::new(Mutex::new(0));

            let alphabet: [char; 16] = [
                '1', '2', '3', '4', '5', '6', '7', '8', '9', '0', 'a', 'b', 'c', 'd', 'e', 'f',
            ];

            fs::create_dir_all(&self.worktree_path)?;

            let available_cpus = num_cpus::get();

            let worktree_pool = Arc::new(Pool::new(available_cpus, || {
                let id = nanoid!(10, &alphabet);

                let handle = repo
                    .create_temp_worktree(&id, &self.worktree_path.join(&id))
                    .unwrap();

                handle
            }));

            // Grouped task by commit, in order of topologial sort
            let visitor = petgraph::visit::Topo::new(&collection_execution_graph.graph);
            let node_indices: Vec<Vec<NodeIndex>> = visitor
                .iter(&collection_execution_graph.graph)
                .fold(indexmap::IndexMap::new(), |mut acc, current| {
                    let task = &collection_execution_graph.graph[current];
                    let entry = acc.entry(task.commit_hash.clone()).or_insert(vec![]);
                    entry.push(current);
                    acc
                })
                .into_iter()
                .map(|(_, task_indices)| task_indices)
                .collect();

            #[cfg(not(feature = "rayon"))]
            let iter = node_indices.iter();
            #[cfg(feature = "rayon")]
            let iter = node_indices.par_iter();

            let disable_cache = self.disable_cache;

            let _: Vec<Result<()>> = iter
            .map(|task_indices| -> Result<()> {
                for task_idx in task_indices {
                    let task = &collection_execution_graph.graph[*task_idx];

                    let is_in_storage = storage
                        .contains_key(&(task.collector_config.clone(), task.commit_hash.clone()));

                    if is_in_storage && disable_cache == false
                    {
                        // TODO: Find better solution for debug logs
                        debug!("Found data from previous run for collector {:?} and commit {}, skipping collection", task.collector_config, task.commit_hash);
                        let mut reused_metric_count_lock = reused_metric_count.lock().unwrap();
                        *reused_metric_count_lock += 1;
                        return Ok(());
                    } else {
                        let collector: Collector = (&task.collector_config).into();

                        let output = match collector {
                            Collector::Base(collector) => {
                                let mut temp_worktree = worktree_pool.try_pull();
                                while temp_worktree.is_none() {
                                    temp_worktree = worktree_pool.try_pull();
                                }
                                let mut temp_worktree = temp_worktree.unwrap();
                                let mut worktree = temp_worktree.as_mut();

                                worktree.reset_hard(&task.commit_hash.0)?;

                                collector.collect(&storage, &mut worktree, &collection_execution_graph, task_idx)?
                            },
                            Collector::Derived(collector) => {
                                collector.collect(&storage, &collection_execution_graph, task_idx)?
                            }
                        };

                        storage.insert(
                            (task.collector_config.clone(), task.commit_hash.clone()),
                            output.clone(),
                        );

                        let mut new_metric_count_lock = new_metric_count.lock().unwrap();

                        *new_metric_count_lock += 1;
                    }

                    let reused_metric_count_lock = reused_metric_count.lock().unwrap();
                    let new_metric_count_lock = new_metric_count.lock().unwrap();

                    pb.inc(1);
                    pb.set_message(format!(
                        "{} collected ({} reused)",
                        *new_metric_count_lock + *reused_metric_count_lock,
                        *reused_metric_count_lock
                    ));
                }

                Ok(())
            })
            .collect();

            drop(worktree_pool);

            pb.finish_and_clear();

            let reused_metric_count = Arc::try_unwrap(reused_metric_count)
                .unwrap()
                .into_inner()
                .unwrap();
            let new_metric_count = Arc::try_unwrap(new_metric_count)
                .unwrap()
                .into_inner()
                .unwrap();

            self.term.clear_last_lines(1)?;
            writeln!(
                self.term,
                "Collected {} data points for {} metrics in {:.2}s ({} reused)",
                new_metric_count + reused_metric_count,
                self.metrics.len(),
                pb.elapsed().as_secs_f32(),
                reused_metric_count
            )?;

            self.state = CollectionProcessState::PostCollection(PostCollection {
                collection_execution_graph,
                storage,
            });
        } else {
            return Err(anyhow::anyhow!("Invalid state"));
        };

        Ok(self)
    }

    pub fn execute_write_to_cache(mut self) -> Result<CollectionProcess<'t>> {
        if let CollectionProcessState::PostCollection(PostCollection {
            collection_execution_graph,
            storage,
        }) = &self.state
        {
            if !self.disable_cache {
                writeln!(self.term, "Writing data to cache")?;
                for nx in collection_execution_graph.graph.node_indices() {
                    let task = &collection_execution_graph.graph[nx];

                    if let Some(value) =
                        storage.get(&(task.collector_config.clone(), task.commit_hash.clone()))
                    {
                        self.cache
                            .store(&task.collector_config, &task.commit_hash, &value)?;
                    }
                }
                self.term.clear_last_lines(1)?;
                writeln!(self.term, "Wrote data to cache")?;
            }
        } else {
            return Err(anyhow::anyhow!("Invalid state"));
        };

        Ok(self)
    }

    pub fn execute_write_to_output(mut self) -> Result<CollectionProcess<'t>> {
        if let CollectionProcessState::PostCollection(PostCollection {
            storage,
            collection_execution_graph: _collection_execution_graph,
        }) = &self.state
        {
            writeln!(self.term, "Writing data to output")?;
            for v in storage {
                let (collector, commit) = v.key();
                let value = v.value();

                let metric_names = self
                    .metrics
                    .iter()
                    .filter(|(_, metric_config)| &metric_config.collector == collector)
                    .map(|(metric_name, _)| metric_name)
                    .collect::<Vec<&String>>();

                for metric_name in metric_names {
                    self.output.set_metric(&metric_name, &commit, &value)?;
                }
            }
            self.output.flush()?;
            self.term.clear_last_lines(1)?;
            writeln!(self.term, "Wrote data to output")?;
        } else {
            return Err(anyhow::anyhow!("Invalid state"));
        };

        Ok(self)
    }

    pub fn execute(self) -> Result<()> {
        let process = self.execute_initial()?;

        let process = match process.state {
            CollectionProcessState::ReadyForFetch(_) => process.execute_fetch()?,
            CollectionProcessState::ReadyForClone(_) => process.execute_clone()?,
            _ => return Err(anyhow::anyhow!("Invalid state")),
        };

        let process = process.execute_collect_commits()?;
        let process = process.execute_collect_tags()?;
        let process = process.execute_prepare_for_collection()?;
        let process = process.execute_collection()?;
        let process = process.execute_write_to_cache()?;
        let process = process.execute_write_to_output()?;

        drop(process);

        Ok(())
    }
}
