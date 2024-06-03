use std::{
    fs::File,
    io::{BufReader, Read, Write},
    path::PathBuf,
};

use anyhow::Result;
use sha1::{Digest, Sha1};

use crate::{config::CollectorConfig, git::CommitHash};

pub trait Cache {
    fn lookup(
        &self,
        collector_config: &CollectorConfig,
        commit_hash: &CommitHash,
    ) -> Result<Option<String>>;

    fn store(
        &self,
        collector_config: &CollectorConfig,
        commit_hash: &CommitHash,
        value: &str,
    ) -> Result<()>;
}

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
    ) -> Result<Option<String>> {
        let file_path = self.get_data_point_path(collector_config, commit_hash)?;

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

    fn store(
        &self,
        collector_config: &CollectorConfig,
        commit_hash: &CommitHash,
        value: &str,
    ) -> Result<()> {
        let file_path = self.get_data_point_path(collector_config, commit_hash)?;

        if let Some(parent) = file_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let mut file = File::create(file_path)?;
        file.write_all(value.as_bytes())?;

        Ok(())
    }
}
