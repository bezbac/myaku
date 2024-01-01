use anyhow::Result;
use clap::{Parser, Subcommand};
use env_logger::Env;
use log::info;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufReader, Read};
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

            info!("{}", tempdir.path().display());
        }
        None => {}
    }

    Ok(())
}
