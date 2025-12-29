mod json;
mod parquet;

pub use json::{JsonOutput, JsonOutputError};
use myaku::{CollectorValue, CommitHash, CommitInfo, CommitTagInfo};
pub use parquet::{ParquetOutput, ParquetOutputError};
use thiserror::Error;

pub trait Output: core::fmt::Debug {
    type Error;

    fn set_commits(&mut self, commits: &[CommitInfo]) -> Result<(), Self::Error>;

    fn set_commit_tags(&mut self, commit_tags: &[CommitTagInfo]) -> Result<(), Self::Error>;

    fn get_metric(
        &self,
        metric_name: &str,
        commit: &CommitHash,
    ) -> Result<Option<CollectorValue>, Self::Error>;
    fn set_metric(
        &mut self,
        metric_name: &str,
        commit: &CommitHash,
        value: &CollectorValue,
    ) -> Result<(), Self::Error>;

    fn load(&self) -> Result<(), Self::Error>;
    fn flush(&self) -> Result<(), Self::Error>;
}

#[derive(Debug, Error)]
pub enum OutputError {
    #[error("{0}")]
    Json(JsonOutputError),
    #[error("{0}")]
    Parquet(ParquetOutputError),
}

impl From<JsonOutputError> for OutputError {
    fn from(value: JsonOutputError) -> Self {
        OutputError::Json(value)
    }
}

impl From<ParquetOutputError> for OutputError {
    fn from(value: ParquetOutputError) -> Self {
        OutputError::Parquet(value)
    }
}

#[derive(Debug)]
pub enum OutputObj {
    Json(JsonOutput),
    Parquet(ParquetOutput),
}

impl Output for OutputObj {
    type Error = OutputError;

    fn set_commits(&mut self, commits: &[CommitInfo]) -> Result<(), Self::Error> {
        match self {
            Self::Json(output) => output.set_commits(commits).map_err(|e| e.into()),
            Self::Parquet(output) => output.set_commits(commits).map_err(|e| e.into()),
        }
    }

    fn set_commit_tags(&mut self, commit_tags: &[CommitTagInfo]) -> Result<(), Self::Error> {
        match self {
            Self::Json(output) => output.set_commit_tags(commit_tags).map_err(|e| e.into()),
            Self::Parquet(output) => output.set_commit_tags(commit_tags).map_err(|e| e.into()),
        }
    }

    fn get_metric(
        &self,
        metric_name: &str,
        commit: &CommitHash,
    ) -> Result<Option<CollectorValue>, Self::Error> {
        match self {
            Self::Json(output) => output.get_metric(metric_name, commit).map_err(|e| e.into()),
            Self::Parquet(output) => output.get_metric(metric_name, commit).map_err(|e| e.into()),
        }
    }

    fn set_metric(
        &mut self,
        metric_name: &str,
        commit: &CommitHash,
        value: &CollectorValue,
    ) -> Result<(), Self::Error> {
        match self {
            Self::Json(output) => output
                .set_metric(metric_name, commit, value)
                .map_err(|e| e.into()),
            Self::Parquet(output) => output
                .set_metric(metric_name, commit, value)
                .map_err(|e| e.into()),
        }
    }

    fn load(&self) -> Result<(), Self::Error> {
        match self {
            Self::Json(output) => output.load().map_err(|e| e.into()),
            Self::Parquet(output) => output.load().map_err(|e| e.into()),
        }
    }

    fn flush(&self) -> Result<(), Self::Error> {
        match self {
            Self::Json(output) => output.flush().map_err(|e| e.into()),
            Self::Parquet(output) => output.flush().map_err(|e| e.into()),
        }
    }
}
