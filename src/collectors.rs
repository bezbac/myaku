use std::collections::BTreeMap;

use anyhow::Result;
use tokei::{LanguageType, Languages};

use crate::{config::CollectorConfig, git::RepositoryHandle};

pub trait Collector {
    fn collect(&self, repo: &RepositoryHandle) -> Result<String>;
}

struct TotalLoc;

impl Collector for TotalLoc {
    fn collect(&self, repo: &RepositoryHandle) -> Result<String> {
        let mut languages = Languages::new();
        languages.get_statistics(&[&repo.path], &[".git"], &tokei::Config::default());
        let value = languages.total().code;
        let result = serde_json::to_string(&value)?;
        Ok(result)
    }
}

struct Loc;

impl Collector for Loc {
    fn collect(&self, repo: &RepositoryHandle) -> Result<String> {
        let mut languages = Languages::new();
        languages.get_statistics(&[&repo.path], &[".git"], &tokei::Config::default());
        let value: BTreeMap<&LanguageType, usize> = languages
            .iter()
            .map(|(lang, info)| (lang, info.code))
            .filter(|(_, value)| *value > 0)
            .collect();
        let result = serde_json::to_string(&value)?;
        Ok(result)
    }
}

struct TotalDiffStat;

impl Collector for TotalDiffStat {
    fn collect(&self, repo: &RepositoryHandle) -> Result<String> {
        let (files_changed, insertions, deletions) =
            repo.get_current_total_diff_stat().unwrap_or((0, 0, 0));

        let result = serde_json::to_string(&(files_changed, insertions, deletions))?;
        Ok(result)
    }
}

impl Collector for CollectorConfig {
    fn collect(&self, repo: &RepositoryHandle) -> Result<String> {
        match self {
            CollectorConfig::Loc => Loc {}.collect(repo),
            CollectorConfig::TotalLoc => TotalLoc {}.collect(repo),
            CollectorConfig::TotalDiffStat => TotalDiffStat {}.collect(repo),
        }
    }
}
