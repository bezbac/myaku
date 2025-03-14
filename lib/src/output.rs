use std::{
    collections::HashMap,
    fs::{self, File},
    io::{BufReader, Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::Result;
use arrow::{
    array::{Array, ArrayRef, RecordBatch, StringArray},
    datatypes::{Field, FieldRef, Schema},
};
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};
use serde_arrow::schema::{SchemaLike, TracingOptions};

use crate::{
    collectors::{
        ChangedFilesLocValue, ChangedFilesValue, CollectorValue, FileListValue, LocValue,
        PatternOccurencesValue, TotalCargoDependenciesValue, TotalDiffStatValue,
        TotalFileCountValue, TotalLocValue, TotalPatternOccurencesValue,
    },
    git::{CommitHash, CommitInfo, CommitTagInfo},
};

pub trait Output: core::fmt::Debug {
    fn set_commits(&mut self, commits: &[CommitInfo]) -> Result<()>;

    fn set_commit_tags(&mut self, commit_tags: &[CommitTagInfo]) -> Result<()>;

    fn get_metric(&self, metric_name: &str, commit: &CommitHash) -> Result<Option<CollectorValue>>;
    fn set_metric(
        &mut self,
        metric_name: &str,
        commit: &CommitHash,
        value: &CollectorValue,
    ) -> Result<()>;

    fn load(&self) -> Result<()>;
    fn flush(&self) -> Result<()>;
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
    fn get_metric(&self, metric_name: &str, commit: &CommitHash) -> Result<Option<CollectorValue>> {
        let file_path = self.get_metric_file(metric_name, commit);

        if !file_path.exists() {
            return Ok(None);
        }

        let file = File::open(file_path).unwrap();
        let mut output = Vec::new();
        let mut reader = BufReader::new(file);

        reader.read_to_end(&mut output)?;

        let contents = String::from_utf8(output)?;

        let value: CollectorValue = serde_json::from_str(&contents)?;

        Ok(Some(value))
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

    fn set_commit_tags(&mut self, commit_tags: &[CommitTagInfo]) -> Result<()> {
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
    ) -> Result<()> {
        let file_path = self.get_metric_file(metric_name, commit);

        if let Some(parent) = file_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let contents = serde_json::to_string(value)?;

        let mut file = File::create(file_path)?;
        file.write_all(contents.as_bytes())?;

        Ok(())
    }

    fn load(&self) -> Result<()> {
        Ok(())
    }

    fn flush(&self) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug)]
pub struct ParquetOutput {
    base: PathBuf,

    // Key: Metric name
    metrics: HashMap<String, HashMap<CommitHash, CollectorValue>>,
}

impl ParquetOutput {
    #[must_use]
    pub fn new(base: &Path) -> Self {
        Self {
            base: base.to_path_buf(),
            metrics: HashMap::default(),
        }
    }
}

impl ParquetOutput {
    fn get_metric_dir(&self, metric_name: &str) -> PathBuf {
        self.base.join("metrics").join(Path::new(metric_name))
    }

    fn get_metric_file(&self, metric_name: &str) -> PathBuf {
        self.get_metric_dir(metric_name).join("data.parquet")
    }

    fn get_writer_props() -> WriterProperties {
        WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build()
    }
}

impl Output for ParquetOutput {
    fn get_metric(&self, metric_name: &str, commit: &CommitHash) -> Result<Option<CollectorValue>> {
        Ok(self
            .metrics
            .get(metric_name)
            .and_then(|metric| metric.get(commit).cloned()))
    }

    fn set_commits(&mut self, commits: &[CommitInfo]) -> Result<()> {
        let file_path: PathBuf = self.base.join("commits.parquet");

        if let Some(parent) = file_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let file = File::create(file_path)?;

        let fields = Vec::<FieldRef>::from_type::<CommitInfo>(
            TracingOptions::default()
                .map_as_struct(false)
                .enums_without_data_as_strings(true)
                .from_type_budget(1000),
        )?;

        let batch = serde_arrow::to_record_batch(&fields, &commits)?;

        let mut writer = ArrowWriter::try_new(
            file,
            batch.schema(),
            Some(ParquetOutput::get_writer_props()),
        )?;

        writer.write(&batch)?;
        writer.close()?;

        Ok(())
    }

    fn set_commit_tags(&mut self, commit_tags: &[CommitTagInfo]) -> Result<()> {
        let file_path: PathBuf = self.base.join("commit_tags.parquet");

        if let Some(parent) = file_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let file = File::create(file_path)?;

        let fields = Vec::<FieldRef>::from_type::<CommitTagInfo>(
            TracingOptions::default()
                .map_as_struct(false)
                .enums_without_data_as_strings(true)
                .from_type_budget(1000),
        )?;

        let batch = serde_arrow::to_record_batch(&fields, &commit_tags)?;

        let mut writer = ArrowWriter::try_new(
            file,
            batch.schema(),
            Some(ParquetOutput::get_writer_props()),
        )?;

        writer.write(&batch)?;
        writer.close()?;

        Ok(())
    }

    fn set_metric(
        &mut self,
        metric_name: &str,
        commit: &CommitHash,
        value: &CollectorValue,
    ) -> Result<()> {
        let metric = self.metrics.entry(metric_name.to_string()).or_default();
        metric.insert(commit.clone(), value.clone());
        Ok(())
    }

    fn flush(&self) -> Result<()> {
        for (metric_name, values) in &self.metrics {
            if values.is_empty() {
                continue;
            }

            let file_path = self.get_metric_file(metric_name);

            if let Some(parent) = file_path.parent() {
                fs::create_dir_all(parent)?;
            }

            let file = File::create(file_path)?;

            let record_batch = values_to_record_batch(values)?;

            let mut writer = ArrowWriter::try_new(
                file,
                record_batch.schema(),
                Some(ParquetOutput::get_writer_props()),
            )?;

            writer.write(&record_batch)?;
            writer.close()?;
        }

        Ok(())
    }

    fn load(&self) -> Result<()> {
        // Not implemented for now, not really needed since we have the cache too
        Ok(())
    }
}

macro_rules! to_batch {
    ($values:expr, $commits:expr, $value_type:ty) => {{
        let mut data: Vec<$value_type> = Vec::new();

        for (commit, value) in $values {
            let inner: $value_type = value
                .clone()
                .try_into()
                .map_err(|_| anyhow::anyhow!("Expected all values to have the same type"))?;
            $commits.push(commit.clone());
            data.push(inner);
        }

        let fields = Vec::<FieldRef>::from_type::<$value_type>(
            TracingOptions::default()
                .map_as_struct(false)
                .enums_without_data_as_strings(true)
                .from_type_budget(1000),
        )?;

        let batch = serde_arrow::to_record_batch(&fields, &data)?;

        batch
    }};
}

fn values_to_record_batch(values: &HashMap<CommitHash, CollectorValue>) -> Result<RecordBatch> {
    let mut commits = Vec::new();

    let Some(first_record) = values.values().next() else {
        return Err(anyhow::anyhow!("No values to convert"));
    };

    let batch = match first_record {
        CollectorValue::TotalDiffStat(_) => {
            to_batch!(values, commits, TotalDiffStatValue)
        }
        CollectorValue::ChangedFiles(_) => {
            to_batch!(values, commits, ChangedFilesValue)
        }
        CollectorValue::Loc(_) => {
            to_batch!(values, commits, LocValue)
        }
        CollectorValue::PatternOccurences(_) => {
            to_batch!(values, commits, PatternOccurencesValue)
        }
        CollectorValue::TotalCargoDependencies(_) => {
            to_batch!(values, commits, TotalCargoDependenciesValue)
        }
        CollectorValue::TotalLoc(_) => {
            to_batch!(values, commits, TotalLocValue)
        }
        CollectorValue::TotalPatternOccurences(_) => {
            to_batch!(values, commits, TotalPatternOccurencesValue)
        }
        CollectorValue::FileList(_) => {
            to_batch!(values, commits, FileListValue)
        }
        CollectorValue::TotalFileCount(_) => {
            to_batch!(values, commits, TotalFileCountValue)
        }
        CollectorValue::ChangedFilesLoc(_) => {
            to_batch!(values, commits, ChangedFilesLocValue)
        }
    };

    let commit_array = StringArray::from(
        commits
            .into_iter()
            .map(|c| c.to_string())
            .collect::<Vec<String>>(),
    );

    let commit_field = Field::new(
        "commit",
        commit_array.data_type().clone(),
        commit_array.is_nullable(),
    );

    let commit_fields: Vec<Arc<Field>> = vec![Arc::new(commit_field)];
    let data_fields: Vec<Arc<Field>> = batch.schema().fields().to_vec();
    let combined_fields: Vec<Arc<Field>> = [commit_fields, data_fields].concat();

    let combined_schema = Schema::new(combined_fields);

    let commit_arrays: Vec<ArrayRef> = vec![Arc::new(commit_array)];
    let data_arrays: Vec<ArrayRef> = batch.columns().to_vec();
    let combined_arrays: Vec<ArrayRef> = [commit_arrays, data_arrays].concat();

    let combined_batch = RecordBatch::try_new(Arc::new(combined_schema), combined_arrays).unwrap();

    Ok(combined_batch)
}
