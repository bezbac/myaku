use std::collections::{HashMap, HashSet};
use std::fs::OpenOptions;
use std::io::{self, Read};
use std::os::fd::AsRawFd;
use std::path::PathBuf;
use std::process::ExitCode;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::{io::Write, time::Duration};

use anyhow::{Ok, Result};
use clap::{Parser, Subcommand};
use console::{colors_enabled, style, Term};
use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};
use myaku::{
    Cache, CollectorConfig, CollectorValue, FileCache, GitRepository, Initial, MetricConfig,
    RepositoryHandle, WorktreeCreationCallbackState,
};
use polars::prelude::*;
use serde::Serialize;
use tracing::debug;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::{prelude::*, registry::Registry};

mod util;

// TODO: Add debug / verbosity flag

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(arg_required_else_help = true)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,

    #[arg(long, action = clap::ArgAction::SetTrue)]
    /// Disable colors
    no_color: bool,

    #[arg(long)]
    /// Enable tracing
    trace: bool,
}

#[derive(Clone, Debug, Default, clap::ValueEnum, Serialize, PartialEq)]
#[serde(rename_all = "kebab-case")]
enum QueryOutputType {
    #[default]
    Csv,
    Jsonl,
    Parquet,
}

// TODO: Merge with lib Frequency enum
#[derive(Clone, Debug, clap::ValueEnum, Serialize, PartialEq)]
#[serde(rename_all = "kebab-case")]
enum Frequency {
    PerCommit,
    Yearly,
    Monthly,
    Weekly,
    Daily,
    Hourly,
}

impl From<&Frequency> for myaku::Frequency {
    fn from(value: &Frequency) -> Self {
        match value {
            Frequency::PerCommit => myaku::Frequency::PerCommit,
            Frequency::Yearly => myaku::Frequency::Yearly,
            Frequency::Monthly => myaku::Frequency::Monthly,
            Frequency::Weekly => myaku::Frequency::Weekly,
            Frequency::Daily => myaku::Frequency::Daily,
            Frequency::Hourly => myaku::Frequency::Hourly,
        }
    }
}

#[allow(clippy::enum_variant_names)]
#[derive(Subcommand)]
enum Query {
    TotalLocOverTime {
        #[arg(long, default_value_t = Frequency::PerCommit, value_enum)]
        frequency: Frequency,
    },
    TotalPatternOccurencesOverTime {
        #[arg(long, default_value_t = Frequency::PerCommit, value_enum)]
        frequency: Frequency,

        #[arg(long)]
        pattern: String,
    },
    TotalContributorCountOverTime,
    TotalDiffByAuthorEmail,
    TotalDiffByAuthorEmailAndFileExtension,
    TotalLocByLanguage,
}

#[derive(Subcommand)]
enum Commands {
    /// Request a singular metric
    Query {
        #[clap(subcommand)]
        query: Query,

        // TODO: Validate: Either of the following:
        // 1. A repository path is provided and contains a git repository (uses existing repo)
        // 2. A repository URL is provided, optionally with a branch (clones into pre-defined directory)
        // 3. A repository URL, branch, and path to non-existent or empty directory are provided (clones into specified directory)
        // 4. A repository URL, branch, and path to existing, non-empty directory are provided (checks if the directory matches the URL and branch, errors if not)
        #[arg(long("url"))]
        repository_url: Option<String>,
        #[arg(long("branch"))]
        repository_branch: Option<String>,
        #[arg(long("path"))]
        repository_path: Option<PathBuf>,

        #[arg(long("output"), default_value_t, value_enum)]
        output_type: QueryOutputType,

        #[arg(short('f'), long("file"))]
        /// Path to output file
        output_file: Option<PathBuf>,

        #[arg(short, long)]
        cache_path: Option<PathBuf>,

        #[arg(short, long, action = clap::ArgAction::SetTrue)]
        no_cache: bool,

        #[arg(short, long, action = clap::ArgAction::SetTrue)]
        offline: bool,

        #[arg(short, long, action = clap::ArgAction::SetTrue, requires = "offline")]
        ignore_mismatched_repo_url: bool,
    },
}

#[derive(Debug)]
struct EmptyTermTarget(io::Empty);

impl EmptyTermTarget {
    pub(crate) fn new() -> Self {
        Self(io::empty())
    }
}

impl AsRawFd for EmptyTermTarget {
    fn as_raw_fd(&self) -> std::os::unix::io::RawFd {
        // Return a dummy file descriptor
        0
    }
}

impl Read for EmptyTermTarget {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.read(buf)
    }
}

impl Write for EmptyTermTarget {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}

fn get_repository_path(repository_name: &str) -> Result<PathBuf> {
    let result = PathBuf::from_str(&format!(".myaku/repositories/{repository_name}"))?;
    Ok(result)
}

#[tracing::instrument]
fn main() -> Result<ExitCode> {
    let cli = Cli::parse();

    let should_render_fancy_output = !cli.trace;
    let should_render_colors = colors_enabled() && !cli.no_color;

    let (term, fmt_layer) = if should_render_fancy_output {
        // TODO: Support the no_color flag
        (Term::stdout(), None)
    } else {
        let user_filter = EnvFilter::builder().try_from_env();

        let (filter, span_level) = if let Result::Ok(user_filter) = user_filter {
            (user_filter, FmtSpan::FULL)
        } else {
            (
                EnvFilter::builder()
                    .with_default_directive("myaku=info".parse().expect("Invalid filter directive"))
                    .from_env_lossy(),
                FmtSpan::ENTER,
            )
        };

        let fmt_subscriber = tracing_subscriber::fmt::layer()
            .with_ansi(should_render_colors)
            .with_span_events(span_level)
            .with_filter(filter)
            .boxed();

        let read = EmptyTermTarget::new();
        let write = EmptyTermTarget::new();

        (Term::read_write_pair(read, write), Some(fmt_subscriber))
    };

    let subscriber = Registry::default().with(fmt_layer);

    tracing::subscriber::set_global_default(subscriber).expect("unable to set global subscriber");

    macro_rules! error {
        ($($arg:tt)*) => {{
            tracing::error!($($arg)*);

            if !should_render_colors {
                writeln!(&term, $($arg)*)
            } else {
                writeln!(&term, "{}", style(format!($($arg)*)).red().bold())
            }
        }};
    }

    macro_rules! info {
        ($($arg:tt)*) => {{
            tracing::info!($($arg)*);
            writeln!(
                &term,
                $($arg)*
            )
        }};
    }

    match &cli.command {
        Some(Commands::Query {
            query,
            output_type,
            output_file,
            repository_url,
            repository_branch,
            repository_path,
            cache_path,
            no_cache: disable_cache,
            offline,
            ignore_mismatched_repo_url,
        }) => {
            if output_type == &QueryOutputType::Parquet && output_file.is_none() {
                error!("Output file must be specified for parquet output")?;
                return Ok(ExitCode::from(1));
            }

            let (reference, reference_dir) = match (repository_url, repository_path) {
                (Some(url), Some(path)) => {
                    let reference = GitRepository {
                        url: url.clone(),
                        branch: repository_branch.clone(),
                    };

                    // TODO: Check if the path exists and is a git repository matching the URL and branch

                    (reference, Some(path.clone()))
                }
                (Some(url), None) => {
                    let reference = GitRepository {
                        url: url.clone(),
                        branch: repository_branch.clone(),
                    };

                    (reference, None)
                }
                (None, Some(path)) => {
                    let handle = RepositoryHandle::open(path)?;

                    let remote_url = handle.remote_url()?;

                    let reference = GitRepository {
                        url: remote_url,
                        branch: None,
                    };

                    (reference, Some(path.clone()))
                }
                (None, None) => {
                    error!("Either a repository URL or a repository path must be provided")?;
                    return Ok(ExitCode::from(1));
                }
            };

            let repository_name =
                util::get_repository_name_from_url(&reference.url).ok_or_else(|| {
                    anyhow::anyhow!(
                        "Cannot determine repository name from URL: {}",
                        &reference.url
                    )
                })?;

            let reference_dir = reference_dir.unwrap_or(get_repository_path(&repository_name)?);

            let cache: Option<Box<dyn Cache>> = if *disable_cache {
                None
            } else {
                let cache_directory = cache_path
                    .clone()
                    .unwrap_or(PathBuf::from(format!(".myaku/cache/{repository_name}")));
                let cache = FileCache::new(&cache_directory);
                Some(Box::new(cache))
            };

            let mut metrics = HashMap::new();

            match query {
                Query::TotalLocOverTime { frequency } => {
                    metrics.insert(
                        "total-loc-over-time".to_string(),
                        MetricConfig {
                            collector: myaku::CollectorConfig::TotalLoc,
                            frequency: frequency.into(),
                        },
                    );
                }
                Query::TotalPatternOccurencesOverTime { pattern, frequency } => {
                    metrics.insert(
                        "total-pattern-occurences-over-time".to_string(),
                        MetricConfig {
                            collector: myaku::CollectorConfig::TotalPatternOccurences {
                                pattern: pattern.clone(),
                                files: None,
                            },
                            frequency: frequency.into(),
                        },
                    );
                }
                Query::TotalDiffByAuthorEmail => {
                    metrics.insert(
                        "total-diff-stat-over-time".to_string(),
                        MetricConfig {
                            collector: myaku::CollectorConfig::TotalDiffStat,
                            frequency: myaku::Frequency::PerCommit,
                        },
                    );
                }
                Query::TotalDiffByAuthorEmailAndFileExtension => {
                    metrics.insert(
                        "changed-files-loc".to_string(),
                        MetricConfig {
                            collector: myaku::CollectorConfig::DiffStat,
                            frequency: myaku::Frequency::PerCommit,
                        },
                    );
                }
                Query::TotalContributorCountOverTime => {}
                Query::TotalLocByLanguage => {
                    metrics.insert(
                        "total-loc-by-language".to_string(),
                        MetricConfig {
                            collector: myaku::CollectorConfig::Loc,
                            frequency: myaku::Frequency::PerCommit,
                        },
                    );
                }
            }

            let process = Initial {
                metrics,

                reference,

                repository_path: reference_dir.clone(),
                cache,

                ssh_key: None,

                offline: *offline,
            }
            .initialize(*ignore_mismatched_repo_url)?;

            info!(
                "Collecting metrics for {}",
                style(&repository_name).underlined()
            )?;

            let process = match process {
                myaku::CollectionProcess::IdleWithoutCommits(process) => {
                    info!("Repository already exists in reference directory")?;
                    info!("Skipped refresh due to --offline argument")?;
                    process
                }
                myaku::CollectionProcess::ReadyForFetch(process) => {
                    info!("Repository already exists in reference directory")?;

                    if *offline {
                        return Err(anyhow::anyhow!(
                            "Cannot fetch repository. Disabled due to --offline argument"
                        ));
                    }

                    info!("Refreshing repository")?;
                    let process = process.fetch()?;
                    term.clear_last_lines(1)?;
                    info!("Refreshed repository successfully")?;
                    process
                }
                myaku::CollectionProcess::ReadyForClone(process) => {
                    info!("Repository does not exist yet in reference directory")?;

                    if *offline {
                        return Err(anyhow::anyhow!(
                            "Cannot clone repository. Disabled due to --offline argument"
                        ));
                    }

                    info!(
                        "Cloning repository into {}",
                        &process.repository_path.display()
                    )?;

                    let pb = ProgressBar::with_draw_target(
                        Some(1000),
                        ProgressDrawTarget::term(term.clone(), 20),
                    );
                    let style = ProgressStyle::with_template(
                        " {spinner} [{elapsed_precise}] [{bar:40}] {msg} (est. {eta} remaining)",
                    )
                    .expect("Failed to create progress style")
                    .progress_chars("#>-");
                    pb.set_style(style);
                    pb.enable_steady_tick(Duration::from_millis(100));
                    pb.set_message("Initializing");
                    let process = process.clone(|progress| match progress {
                        myaku::CloneProgress::EnumeratingObjects => {
                            pb.set_message("Enumerating objects");
                        }
                        myaku::CloneProgress::CountingObjects { finished, total } => {
                            pb.set_message(format!("Counting objects [{finished}, {total}]"));
                            pb.set_length(*total as u64);
                            pb.set_position(*finished as u64);
                        }
                        myaku::CloneProgress::CompressingObjects { finished, total } => {
                            pb.set_message(format!("Compressing objects [{finished}, {total}]",));
                            pb.set_length(*total as u64);
                            pb.set_position(*finished as u64);
                        }
                        myaku::CloneProgress::ReceivingObjects { finished, total } => {
                            pb.set_message(format!("Receiving objects [{finished}, {total}]"));
                            pb.set_length(*total as u64);
                            pb.set_position(*finished as u64);
                        }
                        myaku::CloneProgress::ResolvingDeltas { finished, total } => {
                            pb.set_message(format!("Resolving deltas [{finished}, {total}]",));
                            pb.set_length(*total as u64);
                            pb.set_position(*finished as u64);
                        }
                    })?;
                    pb.finish_and_clear();
                    term.clear_last_lines(1)?;
                    info!(
                        "Successfully cloned repository into {}",
                        &process.get_repository_path().display()
                    )?;

                    process
                }
                _ => return Err(anyhow::anyhow!("Invalid state")),
            };

            info!("Collecting commit information")?;
            let process = process.collect_commits()?;
            term.clear_last_lines(1)?;
            info!("Collected commit information")?;

            info!("Collecting tag information")?;
            let process = process.collect_tags()?;
            term.clear_last_lines(1)?;
            info!("Collected tag information")?;

            info!("Building execution graph")?;
            let process = process.prepare_for_collection(true)?;
            term.clear_last_lines(1)?;
            info!("Built execution graph")?;

            let process = match process.try_fast_forward() {
                std::result::Result::Ok(process) => {
                    info!("Fast-forwarded from previous run")?;
                    process
                }
                std::result::Result::Err(process) => {
                    info!("Creating worktrees")?;
                    let pb = ProgressBar::with_draw_target(
                        Some(1),
                        ProgressDrawTarget::term(term.clone(), 20),
                    );
                    let style = ProgressStyle::with_template(
                        " {spinner} [{elapsed_precise}] [{bar:40}] {msg} (est. {eta} remaining)",
                    )
                    .expect("Failed to create progress style")
                    .progress_chars("#>-");
                    pb.set_style(style);
                    pb.enable_steady_tick(Duration::from_millis(100));
                    let (tx, rx) =
                        std::sync::mpsc::channel::<myaku::WorktreeCreationCallbackState>();
                    let worktree_dir = PathBuf::from(format!(".myaku/worktree/{repository_name}"));
                    let reader = std::thread::spawn(move || {
                        while let Result::Ok(WorktreeCreationCallbackState {
                            desired_worktree_count,
                            ready_worktree_count,
                        }) = rx.recv()
                        {
                            pb.set_length(desired_worktree_count as u64);
                            pb.set_position(ready_worktree_count as u64);
                            pb.set_message(format!(
                                "{ready_worktree_count}/{desired_worktree_count} worktrees created",
                            ));
                        }
                    });
                    let process = process.create_worktrees(Some(tx), worktree_dir)?;
                    reader
                        .join()
                        .map_err(|_| anyhow::anyhow!("Cannot join reader"))?;
                    term.clear_last_lines(1)?;
                    info!("Created worktrees")?;

                    info!("Collecting data points")?;
                    let (
                        process,
                        fresh_task_count,
                        reused_task_count,
                        metric_count,
                        duration_in_secs,
                    ) = {
                        let pb = ProgressBar::with_draw_target(
                            Some(1),
                            ProgressDrawTarget::term(term.clone(), 20),
                        );
                        let style = ProgressStyle::with_template(
                            " {spinner} [{elapsed_precise}] [{bar:40}] {msg} (est. {eta} remaining)",
                        )
                        .expect("Failed to create progress style")
                        .progress_chars("#>-");
                        pb.set_style(style);
                        pb.enable_steady_tick(Duration::from_millis(100));

                        let (tx, rx) =
                            std::sync::mpsc::channel::<myaku::MetricCollectionCallbackState>();

                        let metric_count = Arc::new(Mutex::new(0_usize));
                        let fresh_task_count = Arc::new(Mutex::new(0_usize));
                        let reused_task_count = Arc::new(Mutex::new(0_usize));

                        let movable_pb = pb.clone();
                        let movable_metric_count = metric_count.clone();
                        let movable_fresh_task_count = fresh_task_count.clone();
                        let movable_reused_task_count = reused_task_count.clone();

                        let reader = std::thread::spawn(move || {
                            let pb = movable_pb;
                            let metric_count = movable_metric_count;
                            let fresh_task_count = movable_fresh_task_count;
                            let reused_task_count = movable_reused_task_count;

                            while let Result::Ok(state) = rx.recv() {
                                match state {
                                    myaku::MetricCollectionCallbackState::Initial {
                                        task_count,
                                        metric_count: mcount,
                                    } => {
                                        let mut metric_count_lock = metric_count
                                            .lock()
                                            .expect("Failed to lock metric count");
                                        *metric_count_lock = mcount;
                                        drop(metric_count_lock);
                                        pb.set_length(task_count as u64);
                                    }
                                    myaku::MetricCollectionCallbackState::Reused {
                                        collector_config,
                                        commit_hash,
                                    } => {
                                        debug!("Found data from previous run for collector {:?} and commit {}, skipping collection", collector_config, commit_hash);
                                        let mut reused_task_count_lock = reused_task_count
                                            .lock()
                                            .expect("Failed to lock reused task count");
                                        *reused_task_count_lock += 1;
                                        drop(reused_task_count_lock);
                                    }
                                    myaku::MetricCollectionCallbackState::New {
                                        collector_config: _,
                                        commit_hash: _,
                                    } => {
                                        let mut fresh_task_count_lock = fresh_task_count
                                            .lock()
                                            .expect("Failed to lock fresh task count");
                                        *fresh_task_count_lock += 1;
                                        drop(fresh_task_count_lock);
                                    }
                                    myaku::MetricCollectionCallbackState::Finished => {}
                                }

                                let reused_task_count_lock = reused_task_count
                                    .lock()
                                    .expect("Failed to lock reused task count");
                                let reused_task_count = *reused_task_count_lock;
                                drop(reused_task_count_lock);

                                let fresh_task_count_lock = fresh_task_count
                                    .lock()
                                    .expect("Failed to lock fresh task count");
                                let fresh_task_count = *fresh_task_count_lock;
                                drop(fresh_task_count_lock);

                                pb.inc(1);

                                let total_data_point_count = pb.length().unwrap_or(0);
                                pb.set_message(format!(
                                    "{}/{} collected ({} reused)",
                                    fresh_task_count + reused_task_count,
                                    total_data_point_count,
                                    reused_task_count
                                ));
                            }
                        });

                        let process = process.collect_metrics(Some(tx))?;

                        reader
                            .join()
                            .map_err(|_| anyhow::anyhow!("Cannot join reader"))?;

                        pb.finish_and_clear();
                        let metric_count =
                            *metric_count.lock().expect("Failed to lock metric count");
                        let reused_task_count = *reused_task_count
                            .lock()
                            .expect("Failed to lock reused task count");
                        let fresh_task_count = *fresh_task_count
                            .lock()
                            .expect("Failed to lock fresh task count");

                        let duration_in_secs = pb.elapsed().as_secs_f32();

                        (
                            process,
                            fresh_task_count,
                            reused_task_count,
                            metric_count,
                            duration_in_secs,
                        )
                    };
                    term.clear_last_lines(1)?;

                    info!(
                        "Collected {} data points for {} metrics in {:.2}s ({} reused)",
                        fresh_task_count + reused_task_count,
                        metric_count,
                        duration_in_secs,
                        reused_task_count
                    )?;

                    info!("Writing data to cache")?;
                    let process = process.write_to_cache()?;
                    term.clear_last_lines(1)?;
                    info!("Wrote data to cache")?;

                    process
                }
            };

            let mut df = match query {
                Query::TotalLocOverTime { frequency: _ } => {
                    let mut commit_hashes = vec![];
                    let mut commit_dates = vec![];
                    let mut commit_loc = vec![];

                    for commit in &process.commits {
                        let loc_value = process
                            .storage
                            .get(&(CollectorConfig::TotalLoc, commit.id.clone()));

                        let Some(loc_value) = loc_value else {
                            continue;
                        };

                        let CollectorValue::TotalLoc(loc_value) = loc_value.clone() else {
                            error!("Unexpected collector value")?;
                            return Ok(ExitCode::from(1));
                        };

                        commit_hashes.push(commit.id.0.clone());
                        commit_dates.push(commit.time.timestamp());
                        commit_loc.push(loc_value.loc);
                    }

                    drop(process);

                    DataFrame::new(vec![
                        Column::new("commit_hash".into(), commit_hashes),
                        Column::new("commit_date".into(), commit_dates),
                        Column::new("loc".into(), commit_loc),
                    ])?
                    .sort(
                        ["commit_date"],
                        SortMultipleOptions::new().with_order_descending(true),
                    )?
                }
                Query::TotalPatternOccurencesOverTime {
                    frequency: _,
                    pattern,
                } => {
                    let mut commit_hashes = vec![];
                    let mut commit_dates = vec![];
                    let mut commit_pattern_occurences: Vec<u32> = vec![];

                    for commit in &process.commits {
                        let pattern_occurences_value = process.storage.get(&(
                            CollectorConfig::TotalPatternOccurences {
                                pattern: pattern.clone(),
                                files: None,
                            },
                            commit.id.clone(),
                        ));

                        let Some(pattern_occurences_value) = pattern_occurences_value else {
                            continue;
                        };

                        let CollectorValue::TotalPatternOccurences(pattern_occurences_value) =
                            pattern_occurences_value.clone()
                        else {
                            error!("Unexpected collector value")?;
                            return Ok(ExitCode::from(1));
                        };

                        commit_hashes.push(commit.id.0.clone());
                        commit_dates.push(commit.time.timestamp());
                        commit_pattern_occurences.push(pattern_occurences_value.total_occurences);
                    }

                    drop(process);

                    DataFrame::new(vec![
                        Column::new("commit_hash".into(), commit_hashes),
                        Column::new("commit_date".into(), commit_dates),
                        Column::new("count".into(), commit_pattern_occurences),
                    ])?
                    .sort(
                        ["commit_date"],
                        SortMultipleOptions::new().with_order_descending(true),
                    )?
                }
                Query::TotalDiffByAuthorEmail => {
                    let mut result = HashMap::new();

                    for commit in &process.commits {
                        let diff_stat_value = process
                            .storage
                            .get(&(CollectorConfig::TotalDiffStat, commit.id.clone()));

                        let Some(diff_stat_value) = diff_stat_value else {
                            continue;
                        };

                        let CollectorValue::TotalDiffStat(diff_stat_value) =
                            diff_stat_value.clone()
                        else {
                            error!("Unexpected collector value")?;
                            return Ok(ExitCode::from(1));
                        };

                        if let Some(email) = &commit.author.email {
                            let entry = result.entry(email.clone()).or_insert((0, 0));
                            entry.0 += diff_stat_value.insertions;
                            entry.1 += diff_stat_value.deletions;
                        }
                    }

                    drop(process);

                    let result = result.iter().collect::<Vec<_>>();
                    let emails: Vec<String> =
                        result.iter().map(|(email, _)| (**email).clone()).collect();
                    let added: Vec<u32> = result.iter().map(|(_, (value, _))| *value).collect();
                    let removed: Vec<u32> = result.iter().map(|(_, (_, value))| *value).collect();

                    DataFrame::new(vec![
                        Column::new("emails".into(), emails),
                        Column::new("added".into(), added),
                        Column::new("removed".into(), removed),
                    ])?
                    .sort(
                        ["added"],
                        SortMultipleOptions::new().with_order_descending(true),
                    )?
                }
                Query::TotalDiffByAuthorEmailAndFileExtension => {
                    let mut result = HashMap::new();

                    for commit in &process.commits {
                        let changed_files_diff_stat = process
                            .storage
                            .get(&(CollectorConfig::DiffStat, commit.id.clone()));

                        let Some(changed_files_diff_stat) = changed_files_diff_stat else {
                            continue;
                        };

                        let CollectorValue::DiffStat(changed_files_diff_stat) =
                            changed_files_diff_stat.clone()
                        else {
                            error!("Unexpected collector value")?;
                            return Ok(ExitCode::from(1));
                        };

                        if let Some(email) = &commit.author.email {
                            for (changed_file_path, changed_file_diff_stat) in
                                changed_files_diff_stat.files
                            {
                                let file_extension = changed_file_path
                                    .extension()
                                    .and_then(|v| v.to_str().map(|v| v.to_string()));
                                let entry = result
                                    .entry((email.clone(), file_extension))
                                    .or_insert((0, 0));
                                entry.0 += changed_file_diff_stat.insertions;
                                entry.1 += changed_file_diff_stat.deletions;
                            }
                        }
                    }

                    drop(process);

                    let mut emails: Vec<String> = vec![];
                    let mut file_extensions: Vec<Option<String>> = vec![];
                    let mut added: Vec<u64> = vec![];
                    let mut removed: Vec<u64> = vec![];

                    for ((email, file_extension), (fadded, fremoved)) in result {
                        emails.push(email);
                        file_extensions.push(file_extension);
                        added.push(fadded as u64);
                        removed.push(fremoved as u64);
                    }

                    DataFrame::new(vec![
                        Column::new("email".into(), emails),
                        Column::new("file_extension".into(), file_extensions),
                        Column::new("added".into(), added),
                        Column::new("removed".into(), removed),
                    ])?
                    .sort(
                        ["added"],
                        SortMultipleOptions::new().with_order_descending(true),
                    )?
                }
                Query::TotalContributorCountOverTime => {
                    let mut commit_hashes = vec![];
                    let mut commit_dates = vec![];
                    let mut contributor_counts: Vec<u64> = vec![];
                    let mut emails: HashSet<String> = HashSet::new();

                    for commit in &process.commits {
                        commit_hashes.push(commit.id.0.clone());
                        commit_dates.push(commit.time.timestamp());
                        if let Some(email) = &commit.author.email {
                            emails.insert(email.clone());
                        }
                        contributor_counts.push(emails.len() as u64);
                    }

                    drop(process);

                    DataFrame::new(vec![
                        Column::new("commit_hash".into(), commit_hashes),
                        Column::new("commit_date".into(), commit_dates),
                        Column::new("count".into(), contributor_counts),
                    ])?
                    .sort(
                        ["commit_date"],
                        SortMultipleOptions::new().with_order_descending(true),
                    )?
                }
                Query::TotalLocByLanguage => {
                    let (language, loc) = {
                        let loc_by_language_value = process
                            .storage
                            .get(&(CollectorConfig::Loc, process.latest_commit.clone()));

                        let Some(loc_by_language_value) = loc_by_language_value else {
                            error!("No LOC data found for commit {}", process.latest_commit.0)?;
                            return Ok(ExitCode::from(1));
                        };

                        let CollectorValue::Loc(loc_by_language_value) =
                            loc_by_language_value.clone()
                        else {
                            error!("Unexpected collector value")?;
                            return Ok(ExitCode::from(1));
                        };

                        let mut language = vec![];
                        let mut loc = vec![];

                        for (lang_value, loc_value) in &loc_by_language_value.loc_by_language {
                            language.push(lang_value.to_string());
                            loc.push(*loc_value as u64);
                        }

                        (language, loc)
                    };

                    drop(process);

                    DataFrame::new(vec![
                        Column::new("language".into(), language),
                        Column::new("loc".into(), loc),
                    ])?
                    .sort(
                        ["loc"],
                        SortMultipleOptions::new().with_order_descending(true),
                    )?
                }
            };

            if let Some(output_file) = output_file {
                info!("Writing to output")?;
                let file = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(output_file)?;

                match output_type {
                    QueryOutputType::Csv => {
                        let mut writer = CsvWriter::new(file);
                        writer.finish(&mut df)?;
                    }
                    QueryOutputType::Jsonl => {
                        let mut writer = JsonWriter::new(file);
                        writer.finish(&mut df)?;
                    }
                    QueryOutputType::Parquet => {
                        let writer = ParquetWriter::new(file);
                        writer.finish(&mut df)?;
                    }
                }

                term.clear_last_lines(1)?;
                info!("Wrote output to {}", output_file.display())?;
            } else {
                info!("Result:\n")?;

                match output_type {
                    QueryOutputType::Csv => {
                        let mut writer = CsvWriter::new(term);
                        writer.finish(&mut df)?;
                    }
                    QueryOutputType::Jsonl => {
                        let mut writer = JsonWriter::new(term);
                        writer.finish(&mut df)?;
                    }
                    QueryOutputType::Parquet => {
                        // TODO: Prevent this case on a type level
                        error!("Parquet output requires an output file to be specified")?;
                        return Ok(ExitCode::from(1));
                    }
                }
            }
        }
        None => {}
    }

    Ok(ExitCode::from(0))
}
