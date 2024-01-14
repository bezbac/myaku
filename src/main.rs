use std::fs;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::process::{Command, ExitCode};

use anyhow::{Ok, Result};
use clap::{Parser, Subcommand};
use console::colors_enabled;
use env_logger::fmt::Color;
use env_logger::Env;
use execute::Execute;
use git2::Repository;
use log::{debug, info};
use tokei::Languages;

use crate::config::{Collector, Config};
use crate::git::get_commits;
use crate::output::{FileOutput, Output};

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

    env_logger::Builder::from_env(Env::default().default_filter_or("info"))
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

            let repository_name = util::get_repository_name_from_url(&config.reference.url);
            info!("Collecting metrics for {repository_name}");

            let reference_dir = Path::new("./reference");

            fs::create_dir_all(reference_dir)?;

            let git_path = "git";

            if reference_dir.join(".git").exists() {
                info!("Repository already exists in reference directory");

                // Fetch latest state
                {
                    let mut command = Command::new(git_path);
                    command.current_dir(reference_dir);
                    command.arg("fetch");
                    command.execute_check_exit_status_code(0)?;
                }

                info!("Repository refreshed successfully");
            } else {
                info!(
                    "Cloning repository {repository_name} into {}",
                    &reference_dir.display()
                );

                // Clone repository
                {
                    let mut command = Command::new(git_path);
                    command.arg("clone");
                    command.arg(&config.reference.url);
                    command.arg(&reference_dir);
                    command.execute_check_exit_status_code(0)?;
                }

                info!("Successfully cloned repository");
            };

            let repo = Repository::open(reference_dir)?;

            let branch = match &config.reference.branch {
                Some(branch) => branch,
                None => {
                    let attempts = vec!["master", "main", "dev", "development", "develop"];

                    let mut found = Option::None;

                    for attempt in attempts {
                        match repo
                            .find_branch(&format!("origin/{}", attempt), git2::BranchType::Remote)
                        {
                            Result::Ok(_) => {
                                debug!("Found branch {attempt} in repository");
                                found = Some(attempt);
                                break;
                            }
                            Result::Err(_) => {
                                debug!("Branch {attempt} not found in repository");
                            }
                        }
                    }

                    found.ok_or(anyhow::anyhow!("Could not determine mainline branch"))?
                }
            };

            // Reset to latest commit
            {
                let revstring = format!("origin/{}", branch);
                let (object, _) = repo.revparse_ext(&revstring)?;
                repo.checkout_tree(&object, None)?;
                repo.set_head_detached(object.id())?;
            }

            info!("Collecting commit information");

            let commits = get_commits(&repo)?;

            let mut output = FileOutput::new(outut_directory);

            output.set_commits(&commits)?;

            let mut new_metric_count = 0;
            let mut reused_metric_count = 0;

            for commit_info in &commits {
                let refname = &commit_info.id;

                // Checkout commit
                let (object, _) = repo.revparse_ext(&refname.0)?;
                repo.checkout_tree(&object, None)?;
                repo.set_head_detached(object.id())?;

                for (metric_name, metric) in &config.metrics {
                    if disable_cache == &false {
                        let cached = output.get_metric(metric_name, refname)?;

                        if let Some(_) = cached {
                            debug!("Found data from previous run for metric {metric_name} and commit {refname}, skipping collection");
                            reused_metric_count += 1;
                            continue;
                        }
                    }

                    let metric_value = match metric.collector {
                        Collector::TotalLoc => {
                            let mut languages = Languages::new();
                            languages.get_statistics(
                                &[reference_dir],
                                &[".git"],
                                &tokei::Config::default(),
                            );

                            languages.total().code
                        }
                    };

                    new_metric_count += 1;

                    output.set_metric(metric_name, refname, &metric_value.to_string())?;
                }
            }

            info!(
                "Collected {} data points for {} metrics ({} reused)",
                new_metric_count + reused_metric_count,
                config.metrics.len(),
                reused_metric_count
            )
        }
        None => {}
    }

    Ok(ExitCode::from(0))
}
