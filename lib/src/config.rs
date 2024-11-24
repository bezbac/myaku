use serde::{Deserialize, Serialize};

#[derive(PartialEq, Eq, Hash, Clone, Serialize, Deserialize, Debug)]
#[serde(tag = "collector")]
pub enum CollectorConfig {
    #[serde(rename = "total-loc")]
    TotalLoc,
    #[serde(rename = "loc")]
    Loc,
    #[serde(rename = "total-diff-stat")]
    TotalDiffStat,
    #[serde(rename = "total-cargo-deps")]
    TotalCargoDeps,
    #[serde(rename = "total-pattern-occurences")]
    TotalPatternOccurences { pattern: String },
    #[serde(rename = "pattern-occurences")]
    PatternOccurences { pattern: String },
    #[serde(rename = "changed-files")]
    ChangedFiles,
    #[serde(rename = "file-list")]
    FileList,
    #[serde(rename = "total-file-count")]
    TotalFileCount,
}

#[derive(PartialEq, Eq, Hash, Clone, Serialize, Deserialize, Debug)]
#[serde(rename_all = "kebab-case")]
pub enum Frequency {
    PerCommit,
    Yearly,
    Monthly,
    Weekly,
    Daily,
    Hourly,
}

#[derive(PartialEq, Eq, Hash, Clone, Serialize, Deserialize, Debug)]
pub struct MetricConfig {
    #[serde(flatten)]
    pub collector: CollectorConfig,
    pub frequency: Frequency,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GitRepository {
    pub url: String,
    pub branch: Option<String>,
}
