use std::io::Write;
use std::path::PathBuf;
use std::process::ExitCode;
use std::str::FromStr;

use anyhow::{Ok, Result};
use clap::{Parser, Subcommand};
use console::{colors_enabled, style, Term};
use env_logger::Env;
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

            let mut process = CollectionProcess {
                term: &term,

                reference: config.reference,

                metrics: config.metrics,

                repository_path: reference_dir,
                worktree_path: worktree_dir,
                output,
                cache,

                disable_cache: *disable_cache,
            };

            if let Err(e) = process.execute() {
                write_err(&format!("{}", e))?;
                return Ok(ExitCode::from(1));
            }
        }
        None => {}
    }

    Ok(ExitCode::from(0))
}
