use std::{
    fs::File,
    io::{BufReader, Read, Write},
    path::{Path, PathBuf},
};

use sha1::{Digest, Sha1};
use thiserror::Error;

use crate::{collectors::CollectorValue, config::CollectorConfig, git::CommitHash};

#[derive(Error, Debug)]
pub enum CacheError {
    #[error("IO error: {0}")]
    IO(#[from] std::io::Error),

    #[error("Could not parse string: {0}")]
    StringParsing(#[from] std::string::FromUtf8Error),

    #[error("Serde JSON error: {0}")]
    SerdeJson(#[from] serde_json::Error),

    #[error("Fjall error: {0}")]
    Fjall(#[from] fjall::Error),
}

pub trait Cache {
    fn lookup(
        &self,
        collector_config: &CollectorConfig,
        commit_hash: &CommitHash,
    ) -> Result<Option<CollectorValue>, CacheError>;

    fn store(
        &self,
        collector_config: &CollectorConfig,
        commit_hash: &CommitHash,
        value: &CollectorValue,
    ) -> Result<(), CacheError>;
}

fn get_config_hash(collector_config: &CollectorConfig) -> String {
    let mut hasher = Sha1::new();
    hasher.update(serde_json::to_string(collector_config).unwrap());
    let bytes = hasher.finalize();
    format!("{bytes:x}")
}

#[derive(Debug)]
pub struct FileCache {
    base: PathBuf,
}

impl FileCache {
    #[must_use]
    pub fn new(base: &Path) -> Self {
        Self {
            base: base.to_path_buf(),
        }
    }
}

impl FileCache {
    fn get_data_point_path(
        &self,
        collector_config: &CollectorConfig,
        commit: &CommitHash,
    ) -> Result<PathBuf, CacheError> {
        let config_hash = get_config_hash(collector_config);

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
    ) -> Result<Option<CollectorValue>, CacheError> {
        let file_path = self.get_data_point_path(collector_config, commit_hash)?;

        if !file_path.exists() {
            return Ok(None);
        }

        let file = File::open(file_path)?;
        let mut output = Vec::new();
        let mut reader = BufReader::new(file);

        reader.read_to_end(&mut output)?;

        let contents = String::from_utf8(output)?;

        let value: CollectorValue = serde_json::from_str(&contents)?;

        Ok(Some(value))
    }

    fn store(
        &self,
        collector_config: &CollectorConfig,
        commit_hash: &CommitHash,
        value: &CollectorValue,
    ) -> Result<(), CacheError> {
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

pub struct FjallCache {
    db: fjall::Database,
}

impl FjallCache {
    pub fn new(path: &Path) -> Result<Self, CacheError> {
        let db = fjall::Database::builder(path).open()?;
        Ok(Self { db })
    }

    pub fn get(&self, key: &str) -> Result<Option<fjall::Slice>, CacheError> {
        let items = self
            .db
            .keyspace("items", fjall::KeyspaceCreateOptions::default)?;
        let value = items.get(key)?;
        Ok(value)
    }

    pub fn set(&self, key: &str, value: &[u8]) -> Result<(), CacheError> {
        let items = self
            .db
            .keyspace("items", fjall::KeyspaceCreateOptions::default)?;
        items.insert(key, value)?;
        Ok(())
    }
}

impl Cache for FjallCache {
    fn lookup(
        &self,
        collector_config: &CollectorConfig,
        commit_hash: &CommitHash,
    ) -> Result<Option<CollectorValue>, CacheError> {
        let config_hash = get_config_hash(collector_config);
        let key = format!("{}:{}", config_hash, commit_hash.0);

        if let Some(slice) = self.get(&key)? {
            let contents = String::from_utf8(slice.to_vec())?;

            let value: CollectorValue = serde_json::from_str(&contents)?;

            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    fn store(
        &self,
        collector_config: &CollectorConfig,
        commit_hash: &CommitHash,
        value: &CollectorValue,
    ) -> Result<(), CacheError> {
        let config_hash = get_config_hash(collector_config);
        let key = format!("{}:{}", config_hash, commit_hash.0);

        let contents = serde_json::to_string(value)?;

        self.set(&key, contents.as_bytes())
    }
}
