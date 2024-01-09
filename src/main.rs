use anyhow::{Ok, Result};
use auth_git2::GitAuthenticator;
use clap::{Parser, Subcommand};
use console::colors_enabled;
use env_logger::fmt::Color;
use env_logger::Env;
use git2::{AutotagOption, Repository, Signature, Sort};
use log::{debug, error, info};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Formatter;
use std::fs::{self, File};
use std::io::{BufReader, Read, Write};
use std::path::Path;
use std::path::PathBuf;
use std::process::ExitCode;
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

#[derive(Serialize, Deserialize, Debug)]
struct Author {
    name: Option<String>,
    email: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
struct CommitHash(String);

impl From<String> for CommitHash {
    fn from(item: String) -> Self {
        CommitHash(item)
    }
}

impl std::fmt::Display for CommitHash {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct CommitInfo {
    id: CommitHash,
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
            id: commit.id().to_string().into(),
            author: commit.author().into(),
            committer: commit.committer().into(),
            message: commit.message().map(|v| v.to_string()),
            time: commit.time().seconds(),
        })
    }

    Ok(commits)
}

trait Output {
    fn set_commits(&mut self, commits: &[CommitInfo]) -> Result<()>;

    fn get_metric(&self, metric_name: &str, commit: &CommitHash) -> Result<Option<String>>;
    fn set_metric(&mut self, metric_name: &str, commit: &CommitHash, value: &str) -> Result<()>;
}

struct FileOutput {
    base: PathBuf,
}

impl Default for FileOutput {
    fn default() -> Self {
        Self {
            base: PathBuf::from(".myaku/"),
        }
    }
}

impl FileOutput {
    fn get_metric_dir(&self, metric_name: &str) -> PathBuf {
        self.base.join("metrics").join(Path::new(metric_name))
    }

    fn get_metric_file(&self, metric_name: &str, commit: &CommitHash) -> PathBuf {
        self.get_metric_dir(metric_name)
            .join(Path::new(&format!("{commit}.json")))
    }
}

impl Output for FileOutput {
    fn get_metric(&self, metric_name: &str, commit: &CommitHash) -> Result<Option<String>> {
        let file_path = self.get_metric_file(metric_name, commit);

        if !file_path.exists() {
            return Ok(None);
        }

        let file = File::open(file_path).unwrap();
        let mut output = Vec::new();
        let mut reader = BufReader::new(file);

        reader.read_to_end(&mut output)?;

        let output = String::from_utf8(output)?;

        return Ok(Some(output));
    }

    fn set_commits(&mut self, commits: &[CommitInfo]) -> Result<()> {
        let file_path: PathBuf = self.base.join("commits.json");

        if let Some(parent) = file_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let mut file = File::create(file_path)?;
        let contents: String = serde_json::to_string(&commits)?;
        file.write_all(contents.as_bytes())?;

        Ok(())
    }

    fn set_metric(&mut self, metric_name: &str, commit: &CommitHash, value: &str) -> Result<()> {
        let file_path = self.get_metric_file(metric_name, commit);

        if let Some(parent) = file_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let mut file = File::create(file_path)?;
        let contents = serde_json::to_string(&value)?;
        file.write_all(contents.as_bytes())?;

        Ok(())
    }
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
        }) => {
            let config = load_config(config_path)?;

            info!("Loaded config from {}", config_path.display());

            let repository_name = get_repository_name_from_url(&config.reference.url);

            info!("Collecting metrics for {repository_name}");

            let reference_dir = Path::new("./reference");

            fs::create_dir_all(reference_dir)?;

            let auth = GitAuthenticator::default();
            let git_config = git2::Config::open_default()?;

            let mut fetch_options = git2::FetchOptions::new();
            let mut remote_callbacks = git2::RemoteCallbacks::new();

            remote_callbacks.credentials(auth.credentials(&git_config));
            fetch_options.remote_callbacks(remote_callbacks);

            let repo = if reference_dir.join(".git").exists() {
                info!("Repository already exists in reference directory");

                let repo = Repository::open(reference_dir)?;

                {
                    let remote_name = "origin";

                    let remote = &mut repo.find_remote(remote_name)?;

                    let remote_url = remote
                        .url()
                        .ok_or(anyhow::anyhow!("Remote {} is missing a url", remote_name))?;

                    if remote_url != config.reference.url {
                        error!("Reference repository doesn't match config");
                        return Ok(ExitCode::from(1));
                    }

                    info!("Updating repository");
                    {
                        remote.download(&[] as &[&str], Some(&mut fetch_options))?;
                        remote.disconnect()?;
                        remote.update_tips(None, true, AutotagOption::Unspecified, None)?;
                    }

                    // Reset to latest state of origin

                    let branch = match config.reference.branch {
                        Some(branch) => branch,
                        None => remote.default_branch()?.as_str().unwrap().to_string(),
                    };

                    let refname = format!("origin/{branch}");

                    let (object, _) = repo.revparse_ext(&refname)?;
                    repo.checkout_tree(&object, None)?;
                    repo.set_head_detached(object.id())?;

                    info!("Repository refreshed successfully");
                }

                repo
            } else {
                info!(
                    "Cloning repository {repository_name} into {}",
                    &reference_dir.display()
                );

                let mut repo_builder = git2::build::RepoBuilder::new();
                repo_builder.fetch_options(fetch_options);

                if let Some(branch) = config.reference.branch {
                    repo_builder.branch(&branch);
                }

                let repo = repo_builder.clone(&config.reference.url, reference_dir)?;

                info!("Successfully cloned repository");

                repo
            };

            info!("Collecting commit information");

            let commits = get_commits(&repo)?;

            let mut output = FileOutput::default();

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
