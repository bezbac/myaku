use std::collections::HashMap;
use std::fs::{self};
use std::io::Write;
use std::path::PathBuf;
use std::process::ExitCode;
use std::str::FromStr;
use std::time::Duration;

use anyhow::{Ok, Result};
use cache::Cache;
use clap::{Parser, Subcommand};
use config::CollectorConfig;
use console::{colors_enabled, style, Term};
use env_logger::Env;
use git::CommitHash;
use indicatif::{ProgressBar, ProgressStyle};
use log::debug;
use petgraph::visit::Walker;

use crate::config::Config;
use crate::git::RepositoryHandle;
use crate::git::{clone_repository, CloneProgress};
use crate::graph::build_collection_execution_graph;
use crate::output::{FileOutput, Output};

mod cache;
mod collectors;
mod config;
mod git;
mod graph;
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

// TODO: Add debug / verbosity flag

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

    let term = Term::stdout();

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
            output: outut_directory,
        }) => {
            let config = Config::from_file(config_path)?;

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
            writeln!(
                &term,
                "Collecting metrics for {}",
                style(&repository_name).underlined()
            )?;

            let reference_dir =
                PathBuf::from_str(&format!(".myaku/repositories/{repository_name}"))?;

            fs::create_dir_all(&reference_dir)?;

            let repo = match RepositoryHandle::open(&reference_dir) {
                Result::Ok(repo) => {
                    writeln!(&term, "Repository already exists in reference directory")?;

                    let remote_url = repo.remote_url()?;
                    if remote_url != config.reference.url {
                        write_err("Repository URL in reference directory does not match the one in the config file")?;
                        return Ok(ExitCode::from(1));
                    }

                    writeln!(&term, "Refreshing repository")?;

                    repo.fetch()?;

                    term.clear_last_lines(1)?;
                    writeln!(&term, "Refreshed repository successfully")?;

                    repo
                }
                // TODO: Check for specific error
                Result::Err(_) => {
                    writeln!(
                        &term,
                        "Cloning repository {repository_name} into {}",
                        &reference_dir.display()
                    )?;

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

                    term.clear_last_lines(1)?;
                    writeln!(
                        &term,
                        "Successfully cloned repository into {}",
                        &reference_dir.display()
                    )?;

                    repo
                }
            };

            let branch = match &config.reference.branch {
                Some(branch) => branch.clone(),
                None => repo.find_main_branch()?,
            };

            repo.reset_hard(&format!("origin/{}", branch))?;

            let output_directory = outut_directory
                .clone()
                .unwrap_or(PathBuf::from(format!(".myaku/output/{repository_name}")));
            let mut output = FileOutput::new(&output_directory);

            let cache_directory = PathBuf::from(format!(".myaku/cache/{repository_name}"));
            let cache = cache::FileCache::new(&cache_directory);

            writeln!(&term, "Collecting commit information")?;
            let commits = repo.get_all_commits()?;
            output.set_commits(&commits)?;
            term.clear_last_lines(1)?;
            writeln!(&term, "Collected commit information")?;

            writeln!(&term, "Collecting tag information")?;
            let tags = repo.get_all_commit_tags()?;
            output.set_commit_tags(&tags)?;
            term.clear_last_lines(1)?;
            writeln!(&term, "Collected tag information")?;

            writeln!(&term, "Collecting data points")?;

            let pb = ProgressBar::new(1);
            let style =
                ProgressStyle::with_template(" {spinner} [{elapsed_precise}] [{bar:40}] {msg}")
                    .unwrap()
                    .progress_chars("#>-");
            pb.set_style(style);
            pb.enable_steady_tick(Duration::from_millis(100));
            pb.set_message("Building execution graph");

            let mut storage: HashMap<(CollectorConfig, CommitHash), String> = HashMap::new();

            // Fill storage from cache
            for commit in &commits {
                for (metric_name, metric_config) in &config.metrics {
                    if let Some(value) = output.get_metric(&metric_name, &commit.id)? {
                        storage.insert((metric_config.collector.clone(), commit.id.clone()), value);
                    }
                }
            }

            let collection_execution_graph =
                build_collection_execution_graph(&config.metrics, &commits)?;

            // Fill storage from cache
            for nx in collection_execution_graph.graph.node_indices() {
                let task = &collection_execution_graph.graph[nx];

                if let Some(value) = cache.lookup(&task.collector_config, &task.commit_hash)? {
                    storage.insert(
                        (task.collector_config.clone(), task.commit_hash.clone()),
                        value,
                    );
                }
            }

            let visitor = petgraph::visit::Topo::new(&collection_execution_graph.graph);

            pb.set_length(collection_execution_graph.graph.node_count().try_into()?);

            let mut new_metric_count = 0;
            let mut reused_metric_count = 0;

            for nx in visitor.iter(&collection_execution_graph.graph) {
                let task = &collection_execution_graph.graph[nx];

                if storage.contains_key(&(task.collector_config.clone(), task.commit_hash.clone()))
                    && disable_cache == &false
                {
                    // TODO: Find better solution for debug logs
                    debug!("Found data from previous run for metric {} and commit {}, skipping collection", task.metric_name, task.commit_hash);
                    reused_metric_count += 1;
                    continue;
                } else {
                    repo.reset_hard(&task.commit_hash.0)?;

                    collection_execution_graph.run_task(&mut storage, &nx, &repo)?;

                    new_metric_count += 1;
                }

                pb.inc(1);
                pb.set_message(format!(
                    "{} collected ({} reused)",
                    new_metric_count + reused_metric_count,
                    reused_metric_count
                ));
            }

            pb.finish_and_clear();

            term.clear_last_lines(1)?;
            writeln!(
                &term,
                "Collected {} data points for {} metrics in {:.2}s ({} reused)",
                new_metric_count + reused_metric_count,
                config.metrics.len(),
                pb.elapsed().as_secs_f32(),
                reused_metric_count
            )?;

            writeln!(&term, "Writing data to cache")?;
            for nx in collection_execution_graph.graph.node_indices() {
                let task = &collection_execution_graph.graph[nx];

                if let Some(value) =
                    storage.get(&(task.collector_config.clone(), task.commit_hash.clone()))
                {
                    cache.store(&task.collector_config, &task.commit_hash, value)?;
                }
            }
            term.clear_last_lines(1)?;
            writeln!(&term, "Wrote data to cache")?;

            writeln!(&term, "Writing data to output")?;
            for ((collector, commit), value) in storage {
                let metric_names = config
                    .metrics
                    .iter()
                    .filter(|(_, metric_config)| metric_config.collector == collector)
                    .map(|(metric_name, _)| metric_name)
                    .collect::<Vec<&String>>();

                for metric_name in metric_names {
                    output.set_metric(&metric_name, &commit, &value)?;
                }
            }
            term.clear_last_lines(1)?;
            writeln!(&term, "Wrote data to output")?;
        }
        None => {}
    }

    Ok(ExitCode::from(0))
}
