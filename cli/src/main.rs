use std::path::PathBuf;
use std::process::ExitCode;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::{io::Write, time::Duration};

use anyhow::{Ok, Result};
use clap::{Parser, Subcommand};
use console::{colors_enabled, style, Term};
use env_logger::Env;
use indicatif::{ProgressBar, ProgressStyle};
use log::debug;
use myaku::{Cache, CollectionProcess, FileCache, JsonOutput, Output, ParquetOutput};
use serde::Serialize;

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

        #[arg(long, default_value_t, value_enum)]
        output: OutputType,
    },
}

fn main() -> Result<ExitCode> {
    let cli = Cli::parse();

    let mut term = Term::stdout();

    env_logger::Builder::from_env(
        Env::default().default_filter_or("warn,tokei::language::language_type=off"),
    );

    let write_err = |str: &str| {
        if !colors_enabled() {
            return writeln!(&term, "{str}");
        }

        writeln!(&term, "{}", style(format!("{str}")).red().bold())
    };

    match &cli.command {
        Some(Commands::Collect {
            config: config_path,
            no_cache: disable_cache,
            output: output_type,
        }) => {
            let config = config::Config::from_file(config_path)?;

            writeln!(
                &term,
                "Loaded config from {}",
                style(&config_path.display()).underlined()
            )?;

            if config.metrics.len() == 0 {
                write_err("No metrics configured, please add some to your config file")?;
                return Ok(ExitCode::from(1));
            }

            let repository_name = util::get_repository_name_from_url(&config.reference.url);

            let reference_dir = config.repository_path.unwrap_or(PathBuf::from_str(&format!(
                ".myaku/repositories/{repository_name}"
            ))?);

            let worktree_dir = PathBuf::from(format!(".myaku/worktree/{repository_name}"));

            let output_dir = config
                .output_path
                .unwrap_or(PathBuf::from(format!(".myaku/output/{repository_name}")));

            let output: Box<dyn Output> = match output_type {
                OutputType::Json => Box::new(JsonOutput::new(&output_dir)),
                OutputType::Parquet => Box::new(ParquetOutput::new(&output_dir)),
            };

            let cache_directory = config
                .cache_path
                .unwrap_or(PathBuf::from(format!(".myaku/cache/{repository_name}")));

            let cache = FileCache::new(&cache_directory);
            let cache: Box<dyn Cache> = Box::new(cache);

            writeln!(
                &term,
                "Collecting metrics for {}",
                style(&repository_name).underlined()
            )?;

            let process = CollectionProcess {
                state: myaku::CollectionProcessState::Initial,

                reference: config.reference,

                metrics: config.metrics,

                repository_path: reference_dir.clone(),
                worktree_path: worktree_dir,
                output,
                cache,

                disable_cache: *disable_cache,
            };

            let process = process.execute_initial()?;

            let process = match process.state {
                myaku::CollectionProcessState::ReadyForFetch(_) => {
                    writeln!(term, "Repository already exists in reference directory")?;
                    writeln!(term, "Refreshing repository")?;
                    let process = process.execute_fetch()?;
                    term.clear_last_lines(1)?;
                    writeln!(term, "Refreshed repository successfully")?;
                    process
                }
                myaku::CollectionProcessState::ReadyForClone(_) => {
                    writeln!(term, "Cloning repository into {}", &reference_dir.display())?;

                    let pb = ProgressBar::new(1000);
                    let style = ProgressStyle::with_template(
                        " {spinner} [{elapsed_precise}] [{bar:40}] {msg}",
                    )
                    .unwrap()
                    .progress_chars("#>-");
                    pb.set_style(style);
                    pb.enable_steady_tick(Duration::from_millis(100));
                    pb.set_message("Initializing");
                    let process = process.execute_clone(|progress| match progress {
                        myaku::CloneProgress::EnumeratingObjects => {
                            pb.set_message("Enumerating objects");
                        }
                        myaku::CloneProgress::CountingObjects { finished, total } => {
                            pb.set_message(format!("Counting objects [{}, {}]", finished, total));
                            pb.set_length(*total as u64);
                            pb.set_position(*finished as u64);
                        }
                        myaku::CloneProgress::CompressingObjects { finished, total } => {
                            pb.set_message(format!(
                                "Compressing objects [{}, {}]",
                                finished, total
                            ));
                            pb.set_length(*total as u64);
                            pb.set_position(*finished as u64);
                        }
                        myaku::CloneProgress::ReceivingObjects { finished, total } => {
                            pb.set_message(format!("Receiving objects [{}, {}]", finished, total));
                            pb.set_length(*total as u64);
                            pb.set_position(*finished as u64);
                        }
                        myaku::CloneProgress::ResolvingDeltas { finished, total } => {
                            pb.set_message(format!("Resolving deltas [{}, {}]", finished, total));
                            pb.set_length(*total as u64);
                            pb.set_position(*finished as u64);
                        }
                    })?;
                    pb.finish_and_clear();
                    term.clear_last_lines(1)?;
                    writeln!(
                        term,
                        "Successfully cloned repository into {}",
                        &reference_dir.display()
                    )?;

                    process
                }
                _ => return Err(anyhow::anyhow!("Invalid state")),
            };

            writeln!(term, "Collecting commit information")?;
            let process = process.execute_collect_commits()?;
            term.clear_last_lines(1)?;
            writeln!(term, "Collected commit information")?;

            writeln!(term, "Collecting tag information")?;
            let process = process.execute_collect_tags()?;
            term.clear_last_lines(1)?;
            writeln!(term, "Collected tag information")?;

            writeln!(term, "Building execution graph")?;
            let process = process.execute_prepare_for_collection()?;
            term.clear_last_lines(1)?;
            writeln!(term, "Built execution graph")?;

            writeln!(term, "Collecting data points")?;
            let pb = ProgressBar::new(1);
            let style =
                ProgressStyle::with_template(" {spinner} [{elapsed_precise}] [{bar:40}] {msg}")
                    .unwrap()
                    .progress_chars("#>-");
            pb.set_style(style);
            pb.enable_steady_tick(Duration::from_millis(100));

            let (tx, rx) = std::sync::mpsc::channel::<myaku::ExecutionProgressCallbackState>();

            let metric_count = Arc::new(Mutex::new(0 as usize));
            let fresh_task_count = Arc::new(Mutex::new(0 as usize));
            let reused_task_count = Arc::new(Mutex::new(0 as usize));

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
                            let mut metric_count_lock = metric_count.lock().unwrap();
                            *metric_count_lock = mcount;
                            drop(metric_count_lock);
                            pb.set_length(task_count as u64);
                        }
                        myaku::ExecutionProgressCallbackState::Reused {
                            collector_config,
                            commit_hash,
                        } => {
                            debug!("Found data from previous run for collector {:?} and commit {}, skipping collection", collector_config, commit_hash);
                            let mut reused_task_count_lock = reused_task_count.lock().unwrap();
                            *reused_task_count_lock += 1;
                            drop(reused_task_count_lock);
                        }
                        myaku::ExecutionProgressCallbackState::New {
                            collector_config: _,
                            commit_hash: _,
                        } => {
                            let mut fresh_task_count_lock = fresh_task_count.lock().unwrap();
                            *fresh_task_count_lock += 1;
                            drop(fresh_task_count_lock);
                        }
                        myaku::ExecutionProgressCallbackState::Finished => {}
                    }

                    let reused_task_count_lock = reused_task_count.lock().unwrap();
                    let reused_task_count = *reused_task_count_lock;
                    drop(reused_task_count_lock);

                    let fresh_task_count_lock = fresh_task_count.lock().unwrap();
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

            let process = process.execute_collection(tx)?;

            reader
                .join()
                .map_err(|_| anyhow::anyhow!("Cannot join reader"))?;

            pb.finish_and_clear();
            let metric_count = Arc::try_unwrap(metric_count).unwrap().into_inner().unwrap();
            let reused_task_count = Arc::try_unwrap(reused_task_count)
                .unwrap()
                .into_inner()
                .unwrap();
            let fresh_task_count = Arc::try_unwrap(fresh_task_count)
                .unwrap()
                .into_inner()
                .unwrap();
            term.clear_last_lines(1)?;
            writeln!(
                term,
                "Collected {} data points for {} metrics in {:.2}s ({} reused)",
                fresh_task_count + reused_task_count,
                metric_count,
                pb.elapsed().as_secs_f32(),
                reused_task_count
            )?;

            writeln!(term, "Writing data to cache")?;
            let process = process.execute_write_to_cache()?;
            term.clear_last_lines(1)?;
            writeln!(term, "Wrote data to cache")?;

            writeln!(term, "Writing data to output")?;
            let process = process.execute_write_to_output()?;
            term.clear_last_lines(1)?;
            writeln!(term, "Wrote data to output")?;

            drop(process);
        }
        None => {}
    }

    Ok(ExitCode::from(0))
}
