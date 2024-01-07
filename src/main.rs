use anyhow::Result;
use auth_git2::GitAuthenticator;
use clap::{Parser, Subcommand};
use env_logger::Env;
use git2::{Repository, Signature, Sort};
use log::{debug, info};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufReader, Read, Write};
use std::path::Path;
use std::path::PathBuf;
use tokei::Languages;

#[derive(Serialize, Deserialize, Debug)]
enum Collector {
    #[serde(rename = "total-loc")]
    TotalLoc,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "kebab-case")]
enum Frequency {
    PerCommit,
}

#[derive(Serialize, Deserialize, Debug)]
struct MetricConfig {
    collector: Collector,
    frequency: Frequency,
}

#[derive(Serialize, Deserialize, Debug)]
struct GitRepository {
    url: String,
    branch: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
struct Config {
    reference: GitRepository,
    metrics: HashMap<String, MetricConfig>,
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(arg_required_else_help = true)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Collect metrics
    Collect {
        #[arg(short, long, value_name = "FILE")]
        config: PathBuf,

        #[arg(short, long, action = clap::ArgAction::SetTrue)]
        no_cache: bool,
    },
}

fn load_config(path: &PathBuf) -> Result<Config> {
    let file = File::open(path)?;
    let mut buf_reader = BufReader::new(file);
    let mut contents = String::new();
    buf_reader.read_to_string(&mut contents)?;

    let config: Config = toml::from_str(&contents)?;

    Ok(config)
}

fn get_repository_name_from_url(url: &str) -> String {
    let re =
        Regex::new(r"((git|ssh|http(s)?)|(git@[\w\.]+))(:(//)?)(?<main>[\w\.@\:/\-~]+)(\.git)(/)?")
            .unwrap();
    let caps = re.captures(url).unwrap();
    caps["main"].to_string()
}

#[derive(Serialize, Debug)]
struct Author {
    name: Option<String>,
    email: Option<String>,
}

#[derive(Serialize, Debug)]
struct CommitInfo {
    id: String,
    author: Author,
    committer: Author,
    message: Option<String>,
    time: i64,
}

impl<'a> From<Signature<'a>> for Author {
    fn from(item: Signature) -> Self {
        Author {
            name: item.name().map(|v| v.to_string()),
            email: item.email().map(|v| v.to_string()),
        }
    }
}

fn get_commits(repo: &Repository) -> Result<Vec<CommitInfo>> {
    let mut revwalk = repo.revwalk().unwrap();

    revwalk.set_sorting(Sort::NONE)?;
    revwalk.push_head()?;

    let mut commits: Vec<_> = Vec::new();
    for id in revwalk {
        let oid = id?;
        let commit = repo.find_commit(oid)?;
        commits.push(CommitInfo {
            id: commit.id().to_string(),
            author: commit.author().into(),
            committer: commit.committer().into(),
            message: commit.message().map(|v| v.to_string()),
            time: commit.time().seconds(),
        })
    }

    Ok(commits)
}

fn main() -> Result<()> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    let cli = Cli::parse();

    match &cli.command {
        Some(Commands::Collect {
            config: config_path,
            no_cache: disable_cache,
        }) => {
            let config = load_config(config_path)?;

            info!("Loaded config");

            let repository_name = get_repository_name_from_url(&config.reference.url);

            info!("Collecting metrics for {repository_name}");

            let reference_dir = Path::new("./reference");

            fs::create_dir_all(reference_dir)?;

            info!(
                "Cloning repository {repository_name} into {}",
                &reference_dir.display()
            );

            let auth = GitAuthenticator::default();
            let git_config = git2::Config::open_default()?;
            let mut repo_builder = git2::build::RepoBuilder::new();
            let mut fetch_options = git2::FetchOptions::new();
            let mut remote_callbacks = git2::RemoteCallbacks::new();

            remote_callbacks.credentials(auth.credentials(&git_config));
            fetch_options.remote_callbacks(remote_callbacks);
            repo_builder.fetch_options(fetch_options);

            let repo = repo_builder.clone(&config.reference.url, reference_dir)?;

            info!("Successfully cloned repository");

            info!("Collecting commit information");

            let commits = get_commits(&repo)?;

            let output_dir = Path::new(".myaku");

            fs::create_dir_all(output_dir)?;

            let mut commits_file = File::create(output_dir.join("commits.json"))?;
            let commits_file_content = serde_json::to_string(&commits)?;
            commits_file.write_all(commits_file_content.as_bytes())?;

            let metrics_output_dir = output_dir.join("metrics");

            fs::create_dir_all(&metrics_output_dir)?;

            for commit_info in &commits {
                let refname = &commit_info.id;

                // Checkout commit
                let (object, _) = repo.revparse_ext(&refname)?;
                repo.checkout_tree(&object, None)?;
                repo.set_head_detached(object.id())?;

                for (metric_name, metric) in &config.metrics {
                    let specific_metric_output_dir =
                        metrics_output_dir.join(Path::new(metric_name));

                    let output_file_path =
                        specific_metric_output_dir.join(Path::new(&format!("{refname}.json")));

                    if disable_cache == &false {
                        if output_file_path.exists() {
                            debug!("Found data from previous run for metric {metric_name} and commit {refname}, skipping collection");
                            continue;
                        }
                    }

                    fs::create_dir_all(&specific_metric_output_dir)?;

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

                    let mut result_file = File::create(output_file_path)?;
                    let result_file_content = serde_json::to_string(&metric_value)?;
                    result_file.write_all(result_file_content.as_bytes())?;
                }
            }
        }
        None => {}
    }

    Ok(())
}
