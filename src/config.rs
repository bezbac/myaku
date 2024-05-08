use std::{
    collections::HashMap,
    fs::File,
    io::{BufReader, Read},
    path::PathBuf,
};

use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum Collector {
    #[serde(rename = "total-loc")]
    TotalLoc,
    #[serde(rename = "loc")]
    Loc,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "kebab-case")]
pub enum Frequency {
    PerCommit,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct MetricConfig {
    pub collector: Collector,
    pub frequency: Frequency,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GitRepository {
    pub url: String,
    pub branch: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Config {
    pub reference: GitRepository,
    pub metrics: HashMap<String, MetricConfig>,
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
