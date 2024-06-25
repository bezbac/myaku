use std::{
    collections::HashMap,
    fs::File,
    io::{BufReader, Read},
    path::PathBuf,
};

use anyhow::Result;
use myaku::{GitRepository, MetricConfig};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct Config {
    pub reference: GitRepository,
    pub metrics: HashMap<String, MetricConfig>,

    pub repository_path: Option<PathBuf>,
    pub cache_path: Option<PathBuf>,
    pub output_path: Option<PathBuf>,
}

impl Config {
    pub fn from_file(path: &PathBuf) -> Result<Config> {
        let file = File::open(path)?;
        let mut buf_reader = BufReader::new(file);
        let mut contents = String::new();
        buf_reader.read_to_string(&mut contents)?;

        let config: Config = toml::from_str(&contents)?;

        Ok(config)
    }
}
