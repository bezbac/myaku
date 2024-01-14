use std::{
    fs::{self, File},
    io::{BufReader, Read, Write},
    path::{Path, PathBuf},
};

use anyhow::Result;

use crate::git::{CommitHash, CommitInfo};

pub trait Output {
    fn set_commits(&mut self, commits: &[CommitInfo]) -> Result<()>;

    fn get_metric(&self, metric_name: &str, commit: &CommitHash) -> Result<Option<String>>;
    fn set_metric(&mut self, metric_name: &str, commit: &CommitHash, value: &str) -> Result<()>;
}

pub struct FileOutput {
    base: PathBuf,
}

impl FileOutput {
    pub fn new(base: &Option<PathBuf>) -> Self {
        Self {
            base: base.clone().unwrap_or(PathBuf::from(".myaku/")),
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
