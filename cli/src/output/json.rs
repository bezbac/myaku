use std::{
    fs::{self, File},
    io::{BufReader, Read, Write},
    path::{Path, PathBuf},
};

use myaku::{CollectorValue, CommitHash, CommitInfo, CommitTagInfo};
use thiserror::Error;

use super::Output;

#[derive(Error, Debug)]
pub enum JsonOutputError {
    #[error("IO error: {0}")]
    IO(#[from] std::io::Error),

    #[error("Could not parse string: {0}")]
    StringParsing(#[from] std::string::FromUtf8Error),

    #[error("Serde JSON error: {0}")]
    SerdeJson(#[from] serde_json::Error),
}

#[derive(Debug)]
pub struct JsonOutput {
    base: PathBuf,
}

impl JsonOutput {
    #[must_use]
    pub fn new(base: &Path) -> Self {
        Self {
            base: base.to_path_buf(),
        }
    }
}

impl JsonOutput {
    fn get_metric_dir(&self, metric_name: &str) -> PathBuf {
        self.base.join("metrics").join(Path::new(metric_name))
    }

    fn get_metric_file(&self, metric_name: &str, commit: &CommitHash) -> PathBuf {
        self.get_metric_dir(metric_name)
            .join(Path::new(&format!("{commit}.json")))
    }
}

impl Output for JsonOutput {
    type Error = JsonOutputError;

    fn get_metric(
        &self,
        metric_name: &str,
        commit: &CommitHash,
    ) -> Result<Option<CollectorValue>, Self::Error> {
        let file_path = self.get_metric_file(metric_name, commit);

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

    fn set_commits(&mut self, commits: &[CommitInfo]) -> Result<(), Self::Error> {
        let file_path: PathBuf = self.base.join("commits.json");

        if let Some(parent) = file_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let mut file = File::create(file_path)?;
        let contents: String = serde_json::to_string(&commits)?;
        file.write_all(contents.as_bytes())?;

        Ok(())
    }

    fn set_commit_tags(&mut self, commit_tags: &[CommitTagInfo]) -> Result<(), Self::Error> {
        let file_path: PathBuf = self.base.join("commit_tags.json");

        if let Some(parent) = file_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let mut file = File::create(file_path)?;
        let contents: String = serde_json::to_string(&commit_tags)?;
        file.write_all(contents.as_bytes())?;

        Ok(())
    }

    fn set_metric(
        &mut self,
        metric_name: &str,
        commit: &CommitHash,
        value: &CollectorValue,
    ) -> Result<(), Self::Error> {
        let file_path = self.get_metric_file(metric_name, commit);

        if let Some(parent) = file_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let contents = serde_json::to_string(value)?;

        let mut file = File::create(file_path)?;
        file.write_all(contents.as_bytes())?;

        Ok(())
    }

    fn load(&self) -> Result<(), Self::Error> {
        Ok(())
    }

    fn flush(&self) -> Result<(), Self::Error> {
        Ok(())
    }
}
