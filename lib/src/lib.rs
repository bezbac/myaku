use std::collections::HashMap;
use std::fs::{self};
use std::path::PathBuf;
use std::sync::Arc;

use collectors::{BaseCollector, Collector, DerivedCollector};
use dashmap::DashMap;
use git::GitError;
use graph::CollectionExecutionGraph;
use nanoid::nanoid;
use object_pool::Pool;
use petgraph::graph::NodeIndex;
use petgraph::visit::Walker;
#[cfg(feature = "rayon")]
use rayon::prelude::*;
use ssh_key::PrivateKey;
use thiserror::Error;
use tracing::{debug, span, Level};

use crate::git::clone_repository;
use crate::git::RepositoryHandle;
use crate::graph::build_collection_execution_graph;

mod cache;
mod collectors;
mod config;
mod git;
mod graph;

pub use cache::{Cache, FileCache};
pub use collectors::{
    ChangedFilesLocValue, ChangedFilesValue, CollectorValue, FileListValue, LocValue,
    PatternOccurencesValue, TotalCargoDependenciesValue, TotalDiffStatValue, TotalFileCountValue,
    TotalLocValue, TotalPatternOccurencesValue,
};
pub use config::{CollectorConfig, Frequency, GitRepository, MetricConfig};
pub use git::{CloneProgress, CommitHash, CommitInfo, CommitTagInfo};

#[derive(Error, Debug)]
pub enum CollectionProcessError {
    #[error("No metrics configured")]
    NoMetrics,

    #[error("No commits found")]
    NoCommits,

    #[error("Repository URL in reference directory does not match the one in the config file")]
    MismatchedRepositoryUrl,

    #[error("Invalid state. Expected one of {0:?}")]
    InvalidState(Vec<String>),

    #[error("{0}")]
    BaseCollectorError(#[from] collectors::BaseCollectorError),

    #[error("{0}")]
    DerivedCollectorError(#[from] collectors::DerivedCollectorError),

    #[error("{0}")]
    Cache(#[from] cache::CacheError),

    #[error("{0}")]
    IO(#[from] std::io::Error),

    #[error("{0}")]
    Git(#[from] git::GitError),

    #[error("{0}")]
    Send(#[from] std::sync::mpsc::SendError<ExecutionProgressCallbackState>),
}

pub struct Initial {
    pub metrics: HashMap<String, MetricConfig>,

    shared: SharedCollectionProcessState,
}

pub struct ReadyForClone {
    pub metrics: HashMap<String, MetricConfig>,

    shared: SharedCollectionProcessState,
}

pub struct ReadyForFetch {
    pub metrics: HashMap<String, MetricConfig>,

    shared: SharedCollectionProcessState,
    repo: RepositoryHandle,
}

pub struct IdleWithoutCommits {
    pub metrics: HashMap<String, MetricConfig>,

    shared: SharedCollectionProcessState,
    repo: RepositoryHandle,
}

pub struct IdleWithCommits {
    pub metrics: HashMap<String, MetricConfig>,

    shared: SharedCollectionProcessState,
    repo: RepositoryHandle,
    pub commits: Vec<CommitInfo>,
    pub tags: Option<Vec<CommitTagInfo>>,
    pub storage: DashMap<(CollectorConfig, CommitHash), CollectorValue>,
}

pub struct ReadyForCollection {
    pub metrics: HashMap<String, MetricConfig>,

    shared: SharedCollectionProcessState,
    repo: RepositoryHandle,
    collection_execution_graph: CollectionExecutionGraph,

    pub commits: Vec<CommitInfo>,
    pub tags: Option<Vec<CommitTagInfo>>,
    pub storage: DashMap<(CollectorConfig, CommitHash), CollectorValue>,

    pub latest_commit: CommitHash,
}

pub struct PostCollection {
    pub metrics: HashMap<String, MetricConfig>,

    shared: SharedCollectionProcessState,
    collection_execution_graph: CollectionExecutionGraph,

    pub commits: Vec<CommitInfo>,
    pub tags: Option<Vec<CommitTagInfo>>,
    pub storage: DashMap<(CollectorConfig, CommitHash), CollectorValue>,

    pub latest_commit: CommitHash,
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

#[allow(clippy::struct_excessive_bools)]
pub struct SharedCollectionProcessState {
    pub reference: GitRepository,
    pub repository_path: PathBuf,

    pub ssh_key: Option<PrivateKey>,

    pub worktree_path: PathBuf,

    pub cache: Box<dyn Cache>,

    pub disable_cache: bool,

    /// The `force_latest_commit` flag will be passed to the `build_collection_execution_graph` function
    pub force_latest_commit: bool,

    /// Do not check if the remote URL of the repository matches the one in the config file
    pub ignore_mismatched_repo_url: bool,

    /// If true, do not attempt to perform any network operations (clone, fetch, etc.)
    pub offline: bool,
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
    #[must_use]
    pub fn new(
        metrics: HashMap<String, MetricConfig>,
        shared: SharedCollectionProcessState,
    ) -> Self {
        Self { metrics, shared }
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn initialize(self) -> Result<CollectionProcess, CollectionProcessError> {
        if self.metrics.is_empty() {
            return Err(CollectionProcessError::NoMetrics);
        }

        let reference_dir = &self.shared.repository_path;

        fs::create_dir_all(reference_dir)?;

        return match RepositoryHandle::open(reference_dir) {
            Result::Ok(repo) => {
                let remote_url = repo.remote_url()?;

                if remote_url != self.shared.reference.url
                    && !self.shared.ignore_mismatched_repo_url
                {
                    return Err(CollectionProcessError::MismatchedRepositoryUrl);
                }

                if self.shared.offline {
                    // Skip fetch and clone if offline
                    return Ok(CollectionProcess::IdleWithoutCommits(IdleWithoutCommits {
                        metrics: self.metrics,
                        shared: self.shared,
                        repo,
                    }));
                }

                Ok(CollectionProcess::ReadyForFetch(ReadyForFetch {
                    metrics: self.metrics,
                    repo,
                    shared: self.shared,
                }))
            }
            Err(e) => {
                if self.shared.offline {
                    return Err(CollectionProcessError::Git(e));
                }

                Ok(CollectionProcess::ReadyForClone(ReadyForClone {
                    metrics: self.metrics,
                    shared: self.shared,
                }))
            }
        };
    }

    #[must_use]
    pub fn to_process(self) -> CollectionProcess {
        CollectionProcess::Initial(self)
    }
}

impl ReadyForFetch {
    #[tracing::instrument(level = "trace", skip(self))]
    pub fn fetch(self) -> Result<IdleWithoutCommits, CollectionProcessError> {
        Ok(IdleWithoutCommits {
            metrics: self.metrics,
            shared: self.shared,
            repo: self.repo,
        })
    }
}

impl ReadyForClone {
    #[tracing::instrument(level = "trace", skip(self, callback))]
    pub fn clone(
        self,
        callback: impl Fn(&CloneProgress),
    ) -> Result<IdleWithoutCommits, CollectionProcessError> {
        let repo = clone_repository(
            &self.shared.reference.url,
            &self.shared.repository_path,
            callback,
            self.shared.ssh_key.as_ref(),
        )
        .map_err(|e| CollectionProcessError::Git(GitError::CloneError(e)))?;

        Ok(IdleWithoutCommits {
            metrics: self.metrics,
            shared: self.shared,
            repo,
        })
    }
}

impl IdleWithoutCommits {
    #[tracing::instrument(level = "trace", skip(self))]
    pub fn collect_commits(self) -> Result<IdleWithCommits, CollectionProcessError> {
        let branch = match &self.shared.reference.branch {
            Some(branch) => branch.clone(),
            None => self.repo.find_main_branch()?,
        };

        self.repo.reset_hard(&format!("origin/{branch}"))?;

        let commits = self.repo.get_all_commits()?;

        if commits.is_empty() {
            return Err(CollectionProcessError::NoCommits);
        }

        Ok(IdleWithCommits {
            metrics: self.metrics,
            shared: self.shared,
            repo: self.repo,
            commits,
            tags: None,
            storage: DashMap::new(),
        })
    }
}

impl IdleWithCommits {
    #[tracing::instrument(level = "trace", skip(self))]
    pub fn collect_tags(self) -> Result<IdleWithCommits, CollectionProcessError> {
        let branch = match &self.shared.reference.branch {
            Some(branch) => branch.clone(),
            None => self.repo.find_main_branch()?,
        };

        self.repo.reset_hard(&format!("origin/{branch}"))?;

        let tags = self.repo.get_all_commit_tags()?;

        Ok(IdleWithCommits {
            metrics: self.metrics,
            shared: self.shared,
            repo: self.repo,
            commits: self.commits,
            storage: self.storage,
            tags: Some(tags),
        })
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn prepare_for_collection(self) -> Result<ReadyForCollection, CollectionProcessError> {
        let collection_execution_graph = build_collection_execution_graph(
            &self.metrics,
            &self.commits,
            self.shared.force_latest_commit,
        );

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

        let latest_commit = self
            .commits
            .iter()
            .max_by(|a, b| a.time.cmp(&b.time))
            .map(|c| c.id.clone())
            .ok_or(CollectionProcessError::NoCommits)?;

        Ok(ReadyForCollection {
            metrics: self.metrics,
            shared: self.shared,
            repo: self.repo,
            commits: self.commits,
            tags: self.tags,
            storage: self.storage,
            collection_execution_graph,
            latest_commit,
        })
    }
}

impl ReadyForCollection {
    #[tracing::instrument(level = "trace", skip(self, channel))]
    pub fn collect_metrics(
        self,
        channel: Option<std::sync::mpsc::Sender<ExecutionProgressCallbackState>>,
    ) -> Result<PostCollection, CollectionProcessError> {
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
                .expect("Could not create worktree");

            handle
        }));

        // Grouped task by commit, in order of topologial sort
        let visitor = petgraph::visit::Topo::new(&self.collection_execution_graph.graph);
        let node_indices: Vec<Vec<NodeIndex>> = visitor
            .iter(&self.collection_execution_graph.graph)
            .fold(indexmap::IndexMap::new(), |mut acc, current| {
                let task = &self.collection_execution_graph.graph[current];
                let entry: &mut Vec<NodeIndex> = acc.entry(task.commit_hash.clone()).or_default();
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
                metric_count: self.metrics.len(),
                task_count: self.collection_execution_graph.graph.node_count(),
            })?;
        }

        let _: Vec<Result<(), CollectionProcessError>> = iter
            .cloned()
            .map(|task_indices| -> Result<(), CollectionProcessError> {
                for task_idx in task_indices {
                    let task = &self.collection_execution_graph.graph[task_idx];

                    let _enter =
                        span!(Level::TRACE, "processing task", idx = ?task_idx, commit = ?task.commit_hash).entered();

                    let is_in_storage = self
                        .storage
                        .contains_key(&(task.collector_config.clone(), task.commit_hash.clone()));

                    if is_in_storage && !disable_cache {
                        debug!("reusing value from storage");
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
                                let mut worktree = loop {
                                    if let Some(worktree) = worktree_pool.try_pull() {
                                        break worktree;
                                    }
                                };

                                let worktree = worktree.as_mut();

                                worktree.reset_hard(&task.commit_hash.0)?;
                                collector.collect(
                                    &self.storage,
                                    worktree,
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
            metrics: self.metrics,
            shared: self.shared,
            collection_execution_graph: self.collection_execution_graph,
            commits: self.commits,
            tags: self.tags,
            storage: self.storage,
            latest_commit: self.latest_commit,
        })
    }
}

impl PostCollection {
    #[tracing::instrument(level = "trace", skip(self))]
    pub fn write_to_cache(self) -> Result<PostCollection, CollectionProcessError> {
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
}
