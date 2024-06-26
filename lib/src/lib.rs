use std::collections::HashMap;
use std::fs::{self};
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Ok, Result};
use collectors::{Collector, CollectorValue};
use dashmap::DashMap;
use git::CommitInfo;
use graph::CollectionExecutionGraph;
use nanoid::nanoid;
use object_pool::Pool;
use petgraph::graph::NodeIndex;
use petgraph::visit::Walker;
#[cfg(feature = "rayon")]
use rayon::prelude::*;

use crate::git::clone_repository;
use crate::git::CommitHash;
use crate::git::RepositoryHandle;
use crate::graph::build_collection_execution_graph;

mod cache;
mod collectors;
mod config;
mod git;
mod graph;
mod output;

pub use cache::{Cache, FileCache};
pub use config::{CollectorConfig, GitRepository, MetricConfig};
pub use git::CloneProgress;
pub use output::{JsonOutput, Output, ParquetOutput};

#[derive(Debug)]
pub struct ReadyForClone {}

#[derive(Debug)]
pub struct ReadyForFetch {
    repo: RepositoryHandle,
}

#[derive(Debug)]
pub struct IdleWithoutCommits {
    repo: RepositoryHandle,
}

#[derive(Debug)]
pub struct IdleWithCommits {
    repo: RepositoryHandle,
    commits: Vec<CommitInfo>,
    storage: DashMap<(CollectorConfig, CommitHash), CollectorValue>,
}

#[derive(Debug)]
pub struct ReadyForCollection {
    repo: RepositoryHandle,
    collection_execution_graph: CollectionExecutionGraph,
    storage: DashMap<(CollectorConfig, CommitHash), CollectorValue>,
}

#[derive(Debug)]
pub struct PostCollection {
    collection_execution_graph: CollectionExecutionGraph,
    storage: DashMap<(CollectorConfig, CommitHash), CollectorValue>,
}

#[derive(Debug)]
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

pub struct CollectionProcess {
    pub state: CollectionProcessState,

    pub reference: GitRepository,
    pub repository_path: PathBuf,

    pub metrics: HashMap<String, MetricConfig>,

    pub worktree_path: PathBuf,

    pub output: Box<dyn Output>,
    pub cache: Box<dyn Cache>,

    pub disable_cache: bool,
}

#[derive(Debug)]
pub enum ExecutionProgressCallbackState {
    Initial {
        metric_count: usize,
        task_count: usize,
    },
    New {
        collector_config: CollectorConfig,
        commit_hash: CommitHash,
    },
    Reused {
        collector_config: CollectorConfig,
        commit_hash: CommitHash,
    },
    Finished,
}

impl CollectionProcess {
    #[tracing::instrument(level = "trace", skip(self))]
    pub fn initialize(mut self) -> Result<CollectionProcess> {
        if self.metrics.len() < 1 {
            return Err(anyhow::anyhow!("No metrics configured"));
        }

        return if let CollectionProcessState::Initial = self.state {
            let reference_dir = &self.repository_path;

            fs::create_dir_all(&reference_dir)?;

            match RepositoryHandle::open(&reference_dir) {
                Result::Ok(repo) => {
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

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn fetch(mut self) -> Result<CollectionProcess> {
        return if let CollectionProcessState::ReadyForFetch(ReadyForFetch { repo }) = self.state {
            repo.fetch()?;
            self.state = CollectionProcessState::IdleWithoutCommits(IdleWithoutCommits { repo });

            Ok(self)
        } else {
            Err(anyhow::anyhow!("Invalid state"))
        };
    }

    #[tracing::instrument(level = "trace", skip(self, callback))]
    pub fn clone(mut self, callback: impl Fn(&CloneProgress) -> ()) -> Result<CollectionProcess> {
        return if let CollectionProcessState::ReadyForClone(ReadyForClone {}) = self.state {
            let repo = clone_repository(&self.reference.url, &self.repository_path, callback)?;
            self.state = CollectionProcessState::IdleWithoutCommits(IdleWithoutCommits { repo });

            Ok(self)
        } else {
            Err(anyhow::anyhow!("Invalid state"))
        };
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn skip_fetch(mut self) -> Result<CollectionProcess> {
        return if let CollectionProcessState::ReadyForFetch(ReadyForFetch { repo }) = self.state {
            self.state = CollectionProcessState::IdleWithoutCommits(IdleWithoutCommits { repo });
            Ok(self)
        } else {
            Err(anyhow::anyhow!("Invalid state"))
        };
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn collect_commits(mut self) -> Result<CollectionProcess> {
        if let CollectionProcessState::IdleWithoutCommits(IdleWithoutCommits { repo }) = self.state
        {
            let branch = match &self.reference.branch {
                Some(branch) => branch.clone(),
                None => repo.find_main_branch()?,
            };

            repo.reset_hard(&format!("origin/{}", branch))?;

            let commits = repo.get_all_commits()?;
            self.output.set_commits(&commits)?;

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

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn collect_tags(mut self) -> Result<CollectionProcess> {
        if let CollectionProcessState::IdleWithCommits(IdleWithCommits {
            repo,
            storage: _storage,
            commits: _commits,
        }) = &self.state
        {
            let branch = match &self.reference.branch {
                Some(branch) => branch.clone(),
                None => repo.find_main_branch()?,
            };

            repo.reset_hard(&format!("origin/{}", branch))?;

            let tags = repo.get_all_commit_tags()?;
            self.output.set_commit_tags(&tags)?;
        } else {
            return Err(anyhow::anyhow!("Invalid state"));
        };

        Ok(self)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn prepare_for_collection(mut self) -> Result<CollectionProcess> {
        return if let CollectionProcessState::IdleWithCommits(IdleWithCommits {
            repo,
            commits,
            storage,
        }) = self.state
        {
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

    #[tracing::instrument(level = "trace", skip(self, channel))]
    pub fn collect_metrics(
        mut self,
        channel: std::sync::mpsc::Sender<ExecutionProgressCallbackState>,
    ) -> Result<CollectionProcess> {
        if let CollectionProcessState::ReadyForCollection(ReadyForCollection {
            repo,
            collection_execution_graph,
            storage,
        }) = self.state
        {
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

            channel.send(ExecutionProgressCallbackState::Initial {
                metric_count: self.metrics.len(),
                task_count: collection_execution_graph.graph.node_count().try_into()?,
            })?;

            let _: Vec<Result<()>> = iter
                .map(|task_indices| -> Result<()> {
                    for task_idx in task_indices {
                        let task = &collection_execution_graph.graph[*task_idx];

                        let is_in_storage = storage.contains_key(&(
                            task.collector_config.clone(),
                            task.commit_hash.clone(),
                        ));

                        if is_in_storage && disable_cache == false {
                            channel.send(ExecutionProgressCallbackState::Reused {
                                collector_config: task.collector_config.clone(),
                                commit_hash: task.commit_hash.clone(),
                            })?;
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

                                    collector.collect(
                                        &storage,
                                        &mut worktree,
                                        &collection_execution_graph,
                                        task_idx,
                                    )?
                                }
                                Collector::Derived(collector) => collector.collect(
                                    &storage,
                                    &collection_execution_graph,
                                    task_idx,
                                )?,
                            };

                            storage.insert(
                                (task.collector_config.clone(), task.commit_hash.clone()),
                                output.clone(),
                            );

                            channel.send(ExecutionProgressCallbackState::New {
                                collector_config: task.collector_config.clone(),
                                commit_hash: task.commit_hash.clone(),
                            })?;
                        }
                    }

                    Ok(())
                })
                .collect();

            drop(worktree_pool);

            channel.send(ExecutionProgressCallbackState::Finished)?;

            self.state = CollectionProcessState::PostCollection(PostCollection {
                collection_execution_graph,
                storage,
            });
        } else {
            return Err(anyhow::anyhow!("Invalid state"));
        };

        Ok(self)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn write_to_cache(self) -> Result<CollectionProcess> {
        if let CollectionProcessState::PostCollection(PostCollection {
            collection_execution_graph,
            storage,
        }) = &self.state
        {
            if !self.disable_cache {
                for nx in collection_execution_graph.graph.node_indices() {
                    let task = &collection_execution_graph.graph[nx];

                    if let Some(value) =
                        storage.get(&(task.collector_config.clone(), task.commit_hash.clone()))
                    {
                        self.cache
                            .store(&task.collector_config, &task.commit_hash, &value)?;
                    }
                }
            }
        } else {
            return Err(anyhow::anyhow!("Invalid state"));
        };

        Ok(self)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn write_to_output(mut self) -> Result<CollectionProcess> {
        if let CollectionProcessState::PostCollection(PostCollection {
            storage,
            collection_execution_graph: _collection_execution_graph,
        }) = &self.state
        {
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
        } else {
            return Err(anyhow::anyhow!("Invalid state"));
        };

        Ok(self)
    }
}
