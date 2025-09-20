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
    Cache, FileCache, Initial, JsonOutput, OutputObj, ParquetOutput, SharedCollectionProcessState,
};
use serde::Serialize;
use tracing::debug;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::{prelude::*, registry::Registry};

mod config;
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

#[derive(Clone, Debug, Default, clap::ValueEnum, Serialize)]
#[serde(rename_all = "kebab-case")]
enum OutputType {
    Json,
    #[default]
    Parquet,
}

#[derive(Subcommand)]
enum Commands {
    /// Collect metrics
    Collect {
        #[arg(short, long, value_name = "FILE")]
        config: PathBuf,

        #[arg(short, long, action = clap::ArgAction::SetTrue)]
        no_cache: bool,

        #[arg(short, long, action = clap::ArgAction::SetTrue)]
        offline: bool,

        #[arg(short, long, action = clap::ArgAction::SetTrue, requires = "offline")]
        ignore_mismatched_repo_url: bool,

        #[arg(long, default_value_t, value_enum)]
        output: OutputType,
    },
}

#[derive(Debug)]
struct EmptyTermTarget(io::Empty);

impl EmptyTermTarget {
    pub fn new() -> Self {
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
        Some(Commands::Collect {
            config: config_path,
            no_cache: disable_cache,
            output: output_type,
            offline,
            ignore_mismatched_repo_url,
        }) => {
            let config = config::Config::from_file(config_path)?;

            info!(
                "Loaded config from {}",
                style(&config_path.display()).underlined()
            )?;

            if config.metrics.is_empty() {
                error!("No metrics configured, please add some to your config file")?;
                return Ok(ExitCode::from(1));
            }

            let Some(repository_name) = util::get_repository_name_from_url(&config.reference.url)
            else {
                error!(
                    "Cannot determine repository name from URL: {}",
                    config.reference.url
                )?;
                return Ok(ExitCode::from(1));
            };

            let reference_dir = config.repository_path.unwrap_or(PathBuf::from_str(&format!(
                ".myaku/repositories/{repository_name}"
            ))?);

            let worktree_dir = PathBuf::from(format!(".myaku/worktree/{repository_name}"));

            let output_dir = config
                .output_path
                .unwrap_or(PathBuf::from(format!(".myaku/output/{repository_name}")));

            let output: OutputObj = match output_type {
                OutputType::Json => OutputObj::Json(JsonOutput::new(&output_dir)),
                OutputType::Parquet => OutputObj::Parquet(ParquetOutput::new(&output_dir)),
            };

            let cache_directory = config
                .cache_path
                .unwrap_or(PathBuf::from(format!(".myaku/cache/{repository_name}")));

            let cache = FileCache::new(&cache_directory);
            let cache: Box<dyn Cache> = Box::new(cache);

            info!(
                "Collecting metrics for {}",
                style(&repository_name).underlined()
            )?;

            let process = Initial::new(SharedCollectionProcessState {
                reference: config.reference,

                metrics: config.metrics,

                repository_path: reference_dir.clone(),
                worktree_path: worktree_dir,
                output,
                cache,

                ssh_key: None,

                disable_cache: *disable_cache,

                force_latest_commit: true,
                ignore_mismatched_repo_url: *ignore_mismatched_repo_url,
            })
            .initialize()?;

            let process = match process {
                myaku::CollectionProcess::ReadyForFetch(process) => {
                    info!("Repository already exists in reference directory")?;

                    if *offline {
                        let process = process.skip()?;
                        info!("Skipped refresh due to --offline argument")?;
                        process
                    } else {
                        info!("Refreshing repository")?;
                        let process = process.fetch()?;
                        term.clear_last_lines(1)?;
                        info!("Refreshed repository successfully")?;
                        process
                    }
                }
                myaku::CollectionProcess::ReadyForClone(process) => {
                    if *offline {
                        info!("Repository already exists in reference directory")?;

                        return Err(anyhow::anyhow!(
                            "Cannot clone repository. Disabled due to --offline argument"
                        ));
                    }

                    info!("Cloning repository into {}", &reference_dir.display())?;

                    let pb = ProgressBar::with_draw_target(
                        Some(1000),
                        ProgressDrawTarget::term(term.clone(), 20),
                    );
                    let style = ProgressStyle::with_template(
                        " {spinner} [{elapsed_precise}] [{bar:40}] {msg}",
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
                        &reference_dir.display()
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
            let process = process.prepare_for_collection()?;
            term.clear_last_lines(1)?;
            info!("Built execution graph")?;

            info!("Collecting data points")?;
            let (process, fresh_task_count, reused_task_count, metric_count, duration_in_secs) = {
                let pb = ProgressBar::with_draw_target(
                    Some(1),
                    ProgressDrawTarget::term(term.clone(), 20),
                );
                let style =
                    ProgressStyle::with_template(" {spinner} [{elapsed_precise}] [{bar:40}] {msg}")
                        .expect("Failed to create progress style")
                        .progress_chars("#>-");
                pb.set_style(style);
                pb.enable_steady_tick(Duration::from_millis(100));

                let (tx, rx) = std::sync::mpsc::channel::<myaku::ExecutionProgressCallbackState>();

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
                            myaku::ExecutionProgressCallbackState::Initial {
                                task_count,
                                metric_count: mcount,
                            } => {
                                let mut metric_count_lock =
                                    metric_count.lock().expect("Failed to lock metric count");
                                *metric_count_lock = mcount;
                                drop(metric_count_lock);
                                pb.set_length(task_count as u64);
                            }
                            myaku::ExecutionProgressCallbackState::Reused {
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
                            myaku::ExecutionProgressCallbackState::New {
                                collector_config: _,
                                commit_hash: _,
                            } => {
                                let mut fresh_task_count_lock = fresh_task_count
                                    .lock()
                                    .expect("Failed to lock fresh task count");
                                *fresh_task_count_lock += 1;
                                drop(fresh_task_count_lock);
                            }
                            myaku::ExecutionProgressCallbackState::Finished => {}
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
                        pb.set_message(format!(
                            "{} collected ({} reused)",
                            fresh_task_count + reused_task_count,
                            reused_task_count
                        ));
                    }
                });

                let process = process.collect_metrics(Some(tx))?;

                reader
                    .join()
                    .map_err(|_| anyhow::anyhow!("Cannot join reader"))?;

                pb.finish_and_clear();
                let metric_count = *metric_count.lock().expect("Failed to lock metric count");
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

            info!("Writing data to output")?;
            let process = process.write_to_output()?;
            term.clear_last_lines(1)?;
            info!("Wrote data to output")?;

            drop(process);
        }
        None => {}
    }

    Ok(ExitCode::from(0))
}
