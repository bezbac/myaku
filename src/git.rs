use std::fmt::Formatter;

use anyhow::Result;
use git2::{Repository, Signature, Sort};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct Author {
    pub name: Option<String>,
    pub email: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CommitHash(pub String);

impl From<String> for CommitHash {
    fn from(item: String) -> Self {
        CommitHash(item)
    }
}

impl std::fmt::Display for CommitHash {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CommitInfo {
    pub id: CommitHash,
    pub author: Author,
    pub committer: Author,
    pub message: Option<String>,
    pub time: i64,
}

impl<'a> From<Signature<'a>> for Author {
    fn from(item: Signature) -> Self {
        Author {
            name: item.name().map(|v| v.to_string()),
            email: item.email().map(|v| v.to_string()),
        }
    }
}

pub fn get_commits(repo: &Repository) -> Result<Vec<CommitInfo>> {
    let mut revwalk = repo.revwalk().unwrap();

    revwalk.set_sorting(Sort::NONE)?;
    revwalk.push_head()?;

    let mut commits: Vec<_> = Vec::new();
    for id in revwalk {
        let oid = id?;
        let commit = repo.find_commit(oid)?;
        commits.push(CommitInfo {
            id: commit.id().to_string().into(),
            author: commit.author().into(),
            committer: commit.committer().into(),
            message: commit.message().map(|v| v.to_string()),
            time: commit.time().seconds(),
        })
    }

    Ok(commits)
}
