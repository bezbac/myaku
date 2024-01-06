use anyhow::Result;
use auth_git2::GitAuthenticator;
use clap::{Parser, Subcommand};
use env_logger::Env;
use git2::{Oid, Repository, Signature, Sort};
use log::info;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufReader, Read, Write};
use std::path::Path;
use std::path::PathBuf;
use tempdir::TempDir;

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
        }) => {
            let config = load_config(config_path)?;

            info!("Loaded config");

            let repository_name = get_repository_name_from_url(&config.reference.url);
            let repository_tempdir_name = Regex::new(r"[^a-zA-Z0-9]")
                .unwrap()
                .replace_all(&repository_name, "");

            info!("Collecting metrics for {repository_name}");

            let tempdir = TempDir::new(&repository_tempdir_name)?;

            fs::create_dir_all(tempdir.path())?;

            info!(
                "Cloning repository {repository_name} into {}",
                &tempdir.path().display()
            );

            let auth = GitAuthenticator::default();
            let git_config = git2::Config::open_default()?;
            let mut repo_builder = git2::build::RepoBuilder::new();
            let mut fetch_options = git2::FetchOptions::new();
            let mut remote_callbacks = git2::RemoteCallbacks::new();

            remote_callbacks.credentials(auth.credentials(&git_config));
            fetch_options.remote_callbacks(remote_callbacks);
            repo_builder.fetch_options(fetch_options);

            let repo = repo_builder.clone(&config.reference.url, tempdir.path())?;

            info!("Successfully cloned repository");

            info!("Collecting commit information");

            let commits = get_commits(&repo)?;

            let output_dir = Path::new(".myaku");

            fs::create_dir_all(output_dir)?;

            let mut commits_file = File::create(output_dir.join("commits.json"))?;
            let commits_file_content = serde_json::to_string(&commits)?;
            commits_file.write_all(commits_file_content.as_bytes())?;
        }
        None => {}
    }

    Ok(())
}
