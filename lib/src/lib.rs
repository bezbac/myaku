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
pub use config::{CollectorConfig, Frequency, GitRepository, MetricConfig};
pub use git::CloneProgress;
pub use output::{JsonOutput, Output, ParquetOutput};

pub struct Initial {
    shared: SharedCollectionProcessState,
}

pub struct ReadyForClone {
    shared: SharedCollectionProcessState,
}

pub struct ReadyForFetch {
    shared: SharedCollectionProcessState,
    repo: RepositoryHandle,
}

pub struct IdleWithoutCommits {
    shared: SharedCollectionProcessState,
    repo: RepositoryHandle,
}

pub struct IdleWithCommits {
    shared: SharedCollectionProcessState,
    repo: RepositoryHandle,
    commits: Vec<CommitInfo>,
    storage: DashMap<(CollectorConfig, CommitHash), CollectorValue>,
}

pub struct ReadyForCollection {
    shared: SharedCollectionProcessState,
    repo: RepositoryHandle,
    collection_execution_graph: CollectionExecutionGraph,
    storage: DashMap<(CollectorConfig, CommitHash), CollectorValue>,
}

pub struct PostCollection {
    shared: SharedCollectionProcessState,
    collection_execution_graph: CollectionExecutionGraph,
    storage: DashMap<(CollectorConfig, CommitHash), CollectorValue>,
}

pub enum CollectionProcess {
    /// The collection process has been created but nothing has been executed yet
    Initial(Initial),

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

pub struct SharedCollectionProcessState {
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

impl Initial {
    pub fn new(shared: SharedCollectionProcessState) -> Self {
        Self { shared }
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn initialize(self) -> Result<CollectionProcess> {
        if self.shared.metrics.len() < 1 {
            return Err(anyhow::anyhow!("No metrics configured"));
        }

        let reference_dir = &self.shared.repository_path;

        fs::create_dir_all(&reference_dir)?;

        return match RepositoryHandle::open(&reference_dir) {
            Result::Ok(repo) => {
                let remote_url = repo.remote_url()?;
                if remote_url != self.shared.reference.url {
                    return Err(anyhow::anyhow!("Repository URL in reference directory does not match the one in the config file"));
                }

                Ok(CollectionProcess::ReadyForFetch(ReadyForFetch {
                    repo,
                    shared: self.shared,
                }))
            }
            Err(_) => {
                // TODO: Check specific error
                Ok(CollectionProcess::ReadyForClone(ReadyForClone {
                    shared: self.shared,
                }))
            }
        };
    }

    pub fn to_process(self) -> CollectionProcess {
        CollectionProcess::Initial(self)
    }
}

impl ReadyForFetch {
    #[tracing::instrument(level = "trace", skip(self))]
    pub fn fetch(self) -> Result<IdleWithoutCommits> {
        self.repo.fetch()?;
        Ok(IdleWithoutCommits {
            shared: self.shared,
            repo: self.repo,
        })
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn skip(self) -> Result<IdleWithoutCommits> {
        Ok(IdleWithoutCommits {
            shared: self.shared,
            repo: self.repo,
        })
    }
}

impl ReadyForClone {
    #[tracing::instrument(level = "trace", skip(self, callback))]
    pub fn clone(self, callback: impl Fn(&CloneProgress) -> ()) -> Result<IdleWithoutCommits> {
        let repo = clone_repository(
            &self.shared.reference.url,
            &self.shared.repository_path,
            callback,
        )?;
        Ok(IdleWithoutCommits {
            shared: self.shared,
            repo,
        })
    }
}

impl IdleWithoutCommits {
    #[tracing::instrument(level = "trace", skip(self))]
    pub fn collect_commits(mut self) -> Result<IdleWithCommits> {
        let branch = match &self.shared.reference.branch {
            Some(branch) => branch.clone(),
            None => self.repo.find_main_branch()?,
        };

        self.repo.reset_hard(&format!("origin/{}", branch))?;

        let commits = self.repo.get_all_commits()?;
        self.shared.output.set_commits(&commits)?;

        Ok(IdleWithCommits {
            shared: self.shared,
            repo: self.repo,
            commits,
            storage: DashMap::new(),
        })
    }
}

impl IdleWithCommits {
    #[tracing::instrument(level = "trace", skip(self))]
    pub fn collect_tags(mut self) -> Result<IdleWithCommits> {
        let branch = match &self.shared.reference.branch {
            Some(branch) => branch.clone(),
            None => self.repo.find_main_branch()?,
        };

        self.repo.reset_hard(&format!("origin/{}", branch))?;

        let tags = self.repo.get_all_commit_tags()?;
        self.shared.output.set_commit_tags(&tags)?;

        Ok(self)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn prepare_for_collection(self) -> Result<ReadyForCollection> {
        if !self.shared.disable_cache {
            self.shared.output.load()?;

            // Fill storage from previous output
            for commit in &self.commits {
                for (metric_name, metric_config) in &self.shared.metrics {
                    if let Some(value) = self.shared.output.get_metric(&metric_name, &commit.id)? {
                        self.storage
                            .insert((metric_config.collector.clone(), commit.id.clone()), value);
                    }
                }
            }
        }

        let collection_execution_graph =
            build_collection_execution_graph(&self.shared.metrics, &self.commits)?;

        if !self.shared.disable_cache {
            // Fill storage from cache
            for nx in collection_execution_graph.graph.node_indices() {
                let task = &collection_execution_graph.graph[nx];

                if let Some(value) = self
                    .shared
                    .cache
                    .lookup(&task.collector_config, &task.commit_hash)?
                {
                    self.storage.insert(
                        (task.collector_config.clone(), task.commit_hash.clone()),
                        value,
                    );
                }
            }
        }

        Ok(ReadyForCollection {
            shared: self.shared,
            repo: self.repo,
            storage: self.storage,
            collection_execution_graph,
        })
    }
}

impl ReadyForCollection {
    #[tracing::instrument(level = "trace", skip(self, channel))]
    pub fn collect_metrics(
        self,
        channel: Option<std::sync::mpsc::Sender<ExecutionProgressCallbackState>>,
    ) -> Result<PostCollection> {
        let alphabet: [char; 16] = [
            '1', '2', '3', '4', '5', '6', '7', '8', '9', '0', 'a', 'b', 'c', 'd', 'e', 'f',
        ];

        fs::create_dir_all(&self.shared.worktree_path)?;

        let available_cpus = num_cpus::get();

        let worktree_pool = Arc::new(Pool::new(available_cpus, || {
            let id = nanoid!(10, &alphabet);

            let handle = self
                .repo
                .create_temp_worktree(&id, &self.shared.worktree_path.join(&id))
                .unwrap();

            handle
        }));

        // Grouped task by commit, in order of topologial sort
        let visitor = petgraph::visit::Topo::new(&self.collection_execution_graph.graph);
        let node_indices: Vec<Vec<NodeIndex>> = visitor
            .iter(&self.collection_execution_graph.graph)
            .fold(indexmap::IndexMap::new(), |mut acc, current| {
                let task = &self.collection_execution_graph.graph[current];
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

        let disable_cache = self.shared.disable_cache;

        if let Some(channel) = &channel {
            channel.send(ExecutionProgressCallbackState::Initial {
                metric_count: self.shared.metrics.len(),
                task_count: self
                    .collection_execution_graph
                    .graph
                    .node_count()
                    .try_into()?,
            })?;
        }

        let _: Vec<Result<()>> = iter
            .map(|task_indices| -> Result<()> {
                for task_idx in task_indices {
                    let task = &self.collection_execution_graph.graph[*task_idx];

                    let is_in_storage = self
                        .storage
                        .contains_key(&(task.collector_config.clone(), task.commit_hash.clone()));

                    if is_in_storage && disable_cache == false {
                        if let Some(channel) = &channel {
                            channel.send(ExecutionProgressCallbackState::Reused {
                                collector_config: task.collector_config.clone(),
                                commit_hash: task.commit_hash.clone(),
                            })?;
                        }
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
                                    &self.storage,
                                    &mut worktree,
                                    &self.collection_execution_graph,
                                    task_idx,
                                )?
                            }
                            Collector::Derived(collector) => collector.collect(
                                &self.storage,
                                &self.collection_execution_graph,
                                task_idx,
                            )?,
                        };

                        self.storage.insert(
                            (task.collector_config.clone(), task.commit_hash.clone()),
                            output.clone(),
                        );

                        if let Some(channel) = &channel {
                            channel.send(ExecutionProgressCallbackState::New {
                                collector_config: task.collector_config.clone(),
                                commit_hash: task.commit_hash.clone(),
                            })?;
                        }
                    }
                }

                Ok(())
            })
            .collect();

        drop(worktree_pool);

        if let Some(channel) = &channel {
            channel.send(ExecutionProgressCallbackState::Finished)?;
        }

        Ok(PostCollection {
            shared: self.shared,
            collection_execution_graph: self.collection_execution_graph,
            storage: self.storage,
        })
    }
}

impl PostCollection {
    #[tracing::instrument(level = "trace", skip(self))]
    pub fn write_to_cache(self) -> Result<PostCollection> {
        if !self.shared.disable_cache {
            for nx in self.collection_execution_graph.graph.node_indices() {
                let task = &self.collection_execution_graph.graph[nx];

                if let Some(value) = self
                    .storage
                    .get(&(task.collector_config.clone(), task.commit_hash.clone()))
                {
                    self.shared
                        .cache
                        .store(&task.collector_config, &task.commit_hash, &value)?;
                }
            }
        }

        Ok(self)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn write_to_output(mut self) -> Result<PostCollection> {
        for e in &self.storage {
            let (collector, commit) = e.key();
            let value = e.value();
            let metric_names = self
                .shared
                .metrics
                .iter()
                .filter(|(_, metric_config)| &metric_config.collector == collector)
                .map(|(metric_name, _)| metric_name)
                .collect::<Vec<&String>>();

            for metric_name in metric_names {
                self.shared
                    .output
                    .set_metric(&metric_name, &commit, &value)?;
            }
        }

        self.shared.output.flush()?;

        Ok(self)
    }
}

impl CollectionProcess {
    pub fn run_to_completion(self) -> Result<()> {
        let process = if let CollectionProcess::Initial(process) = self {
            process
        } else {
            return Err(anyhow::anyhow!("Invalid state"));
        };

        let process = process.initialize()?;

        let process = match process {
            CollectionProcess::ReadyForFetch(process) => process.fetch()?,
            CollectionProcess::ReadyForClone(process) => process.clone(|_| {})?,
            _ => return Err(anyhow::anyhow!("Invalid state")),
        };

        process
            .collect_commits()?
            .collect_tags()?
            .prepare_for_collection()?
            .collect_metrics(None)?
            .write_to_cache()?
            .write_to_output()?;

        Ok(())
    }
}
