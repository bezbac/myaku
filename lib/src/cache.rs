use std::{
    fs::File,
    io::{BufReader, Read, Write},
    path::PathBuf,
};

use anyhow::Result;
use sha1::{Digest, Sha1};

use crate::{collectors::CollectorValue, config::CollectorConfig, git::CommitHash};

pub trait Cache: core::fmt::Debug {
    fn lookup(
        &self,
        collector_config: &CollectorConfig,
        commit_hash: &CommitHash,
    ) -> Result<Option<CollectorValue>>;

    fn store(
        &self,
        collector_config: &CollectorConfig,
        commit_hash: &CommitHash,
        value: &CollectorValue,
    ) -> Result<()>;
}

#[derive(Debug)]
pub struct FileCache {
    base: PathBuf,
}

impl FileCache {
    pub fn new(base: &PathBuf) -> Self {
        Self { base: base.clone() }
    }
}

impl FileCache {
    fn get_data_point_path(
        &self,
        collector_config: &CollectorConfig,
        commit: &CommitHash,
    ) -> Result<PathBuf> {
        let config_hash = {
            let mut hasher = Sha1::new();
            hasher.update(serde_json::to_string(collector_config)?);
            let bytes = hasher.finalize();
            format!("{:x}", bytes)
        };

        let mut path = self
            .base
            .join(PathBuf::from(config_hash))
            .join(PathBuf::from(&commit.0));

        path.set_extension("json");

        Ok(path)
    }
}

impl Cache for FileCache {
    fn lookup(
        &self,
        collector_config: &CollectorConfig,
        commit_hash: &CommitHash,
    ) -> Result<Option<CollectorValue>> {
        let file_path = self.get_data_point_path(collector_config, commit_hash)?;

        if !file_path.exists() {
            return Ok(None);
        }

        let file = File::open(file_path).unwrap();
        let mut output = Vec::new();
        let mut reader = BufReader::new(file);

        reader.read_to_end(&mut output)?;

        let contents = String::from_utf8(output)?;

        let value: CollectorValue = serde_json::from_str(&contents)?;

        return Ok(Some(value));
    }

    fn store(
        &self,
        collector_config: &CollectorConfig,
        commit_hash: &CommitHash,
        value: &CollectorValue,
    ) -> Result<()> {
        let file_path = self.get_data_point_path(collector_config, commit_hash)?;

        if let Some(parent) = file_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let contents = serde_json::to_string(value)?;

        let mut file = File::create(file_path)?;
        file.write_all(contents.as_bytes())?;

        Ok(())
    }
}
