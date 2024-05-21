use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::process::ExitCode;
use std::str::FromStr;
use std::time::Duration;

use anyhow::{Ok, Result};
use clap::{Parser, Subcommand};
use console::colors_enabled;
use env_logger::fmt::Color;
use env_logger::Env;
use indicatif::{ProgressBar, ProgressStyle};
use log::{debug, error, info};

use crate::collectors::Collector;
use crate::config::Config;
use crate::git::RepositoryHandle;
use crate::git::{clone_repository, CloneProgress};
use crate::output::{FileOutput, Output};

mod collectors;
mod config;
mod git;
mod output;
mod util;

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

#[derive(Subcommand)]
enum Commands {
    /// Collect metrics
    Collect {
        #[arg(short, long, value_name = "FILE")]
        config: PathBuf,

        #[arg(short, long)]
        output: Option<PathBuf>,

        #[arg(short, long, action = clap::ArgAction::SetTrue)]
        no_cache: bool,
    },
}

fn main() -> Result<ExitCode> {
    let cli = Cli::parse();

    env_logger::Builder::from_env(
        Env::default().default_filter_or("info,tokei::language::language_type=off"),
    )
    .format(move |buf, record| {
        let mut style = buf.style();

        if colors_enabled() && !cli.no_color {
            if record.level() == log::Level::Warn {
                style.set_color(Color::Yellow).set_bold(true);
            }

            if record.level() == log::Level::Error {
                style.set_color(Color::Red).set_bold(true);
            }

            if record.level() == log::Level::Debug {
                style.set_color(Color::Ansi256(240));
            }
        }

        writeln!(buf, "{}", style.value(record.args()))
    })
    .init();

    match &cli.command {
        Some(Commands::Collect {
            config: config_path,
            no_cache: disable_cache,
            output: outut_directory,
        }) => {
            let config = Config::from_file(config_path)?;

            info!("Loaded config from {}", config_path.display());

            if config.metrics.len() == 0 {
                error!("No metrics configured, please add some to your config file");
                return Ok(ExitCode::from(1));
            }

            let repository_name = util::get_repository_name_from_url(&config.reference.url);
            info!("Collecting metrics for {repository_name}");

            let reference_dir = PathBuf::from_str("./reference")?;

            fs::create_dir_all(&reference_dir)?;

            let repo = match RepositoryHandle::open(&reference_dir) {
                Result::Ok(repo) => {
                    info!("Repository already exists in reference directory");

                    let remote_url = repo.remote_url()?;
                    if remote_url != config.reference.url {
                        error!("Repository URL in reference directory does not match the one in the config file");
                        return Ok(ExitCode::from(1));
                    }

                    repo.fetch()?;

                    info!("Repository refreshed successfully");

                    repo
                }
                // TODO: Check for specific error
                Result::Err(_) => {
                    info!(
                        "Cloning repository {repository_name} into {}",
                        &reference_dir.display()
                    );

                    let pb = ProgressBar::new(1000);
                    let style = ProgressStyle::with_template(
                        " {spinner} [{elapsed_precise}] [{bar:40}] {msg}",
                    )
                    .unwrap()
                    .progress_chars("#>-");
                    pb.set_style(style);
                    pb.enable_steady_tick(Duration::from_millis(100));

                    pb.set_message("Initializing");

                    let repo =
                        clone_repository(&config.reference.url, &reference_dir, |progress| {
                            let bar = &pb;

                            match progress {
                                CloneProgress::EnumeratingObjects => {
                                    bar.set_message("Enumerating objects");
                                }
                                CloneProgress::CountingObjects { finished, total } => {
                                    bar.set_message(format!(
                                        "Counting objects [{}, {}]",
                                        finished, total
                                    ));
                                    bar.set_length(*total as u64);
                                    bar.set_position(*finished as u64);
                                }
                                CloneProgress::CompressingObjects { finished, total } => {
                                    bar.set_message(format!(
                                        "Compressing objects [{}, {}]",
                                        finished, total
                                    ));
                                    bar.set_length(*total as u64);
                                    bar.set_position(*finished as u64);
                                }
                                CloneProgress::ReceivingObjects { finished, total } => {
                                    bar.set_message(format!(
                                        "Receiving objects [{}, {}]",
                                        finished, total
                                    ));
                                    bar.set_length(*total as u64);
                                    bar.set_position(*finished as u64);
                                }
                                CloneProgress::ResolvingDeltas { finished, total } => {
                                    bar.set_message(format!(
                                        "Resolving deltas [{}, {}]",
                                        finished, total
                                    ));
                                    bar.set_length(*total as u64);
                                    bar.set_position(*finished as u64);
                                }
                            }
                        })?;

                    pb.finish_and_clear();

                    info!("Successfully cloned repository");

                    repo
                }
            };

            let branch = match &config.reference.branch {
                Some(branch) => branch.clone(),
                None => repo.find_main_branch()?,
            };

            repo.reset_hard(&format!("origin/{}", branch))?;

            info!("Collecting commit information");

            let commits = repo.get_all_commits()?;

            let output_directory = outut_directory
                .clone()
                .unwrap_or(PathBuf::from(format!(".myaku/output/{repository_name}")));

            let mut output = FileOutput::new(&output_directory);

            output.set_commits(&commits)?;

            info!("Collecting tag information");

            let tags = repo.get_all_commit_tags()?;

            output.set_commit_tags(&tags)?;

            info!("Collecting metrics");

            let mut new_metric_count = 0;
            let mut reused_metric_count = 0;

            let pb = ProgressBar::new((commits.len() * config.metrics.len()) as u64);
            let style =
                ProgressStyle::with_template(" {spinner} [{elapsed_precise}] [{bar:40}] {msg}")
                    .unwrap()
                    .progress_chars("#>-");
            pb.set_style(style);
            pb.enable_steady_tick(Duration::from_millis(100));

            pb.set_message("Initializing");

            for commit_info in &commits {
                let refname = &commit_info.id;

                repo.reset_hard(&refname.0)?;

                for (metric_name, metric) in &config.metrics {
                    if disable_cache == &false {
                        let cached = output.get_metric(metric_name, refname)?;

                        if let Some(_) = cached {
                            debug!("Found data from previous run for metric {metric_name} and commit {refname}, skipping collection");
                            reused_metric_count += 1;
                            continue;
                        }
                    }

                    let metric_value = metric.collector.collect(&repo)?;

                    output.set_metric(metric_name, refname, &metric_value.to_string())?;

                    new_metric_count += 1;
                    pb.inc(1);
                    pb.set_message(format!(
                        "{} collected ({} reused)",
                        new_metric_count + reused_metric_count,
                        reused_metric_count
                    ));
                }
            }

            pb.finish_and_clear();

            info!(
                "Collected {} data points for {} metrics in {:.2}s ({} reused)",
                new_metric_count + reused_metric_count,
                config.metrics.len(),
                pb.elapsed().as_secs_f32(),
                reused_metric_count
            )
        }
        None => {}
    }

    Ok(ExitCode::from(0))
}
