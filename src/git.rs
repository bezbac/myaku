use std::{fmt::Formatter, path::PathBuf, process::Command};

use anyhow::Result;
use execute::Execute;
use git2::{Repository, Signature, Sort};
use log::debug;
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

const GIT_BINARY_PATH: &str = "git";

pub struct RepositoryHandle {
    path: PathBuf,
}

impl From<&RepositoryHandle> for Repository {
    fn from(value: &RepositoryHandle) -> Self {
        Repository::open(&value.path).unwrap()
    }
}

impl RepositoryHandle {
    pub fn open(path: &PathBuf) -> Result<RepositoryHandle> {
        if path.join(".git").exists() {
            return Ok(RepositoryHandle { path: path.clone() });
        }

        Err(anyhow::anyhow!(".git directory does not exist"))
    }

    pub fn fetch(&self) -> Result<()> {
        let mut command = Command::new(GIT_BINARY_PATH);
        command.current_dir(&self.path);
        command.arg("fetch");
        command.execute_check_exit_status_code(0)?;

        Ok(())
    }

    pub fn find_main_branch(&self) -> Result<String> {
        let git2_repo: Repository = self.into();

        let mut found = Option::None;
        for attempt in &["master", "main", "dev", "development", "develop"] {
            match git2_repo.find_branch(&format!("origin/{}", attempt), git2::BranchType::Remote) {
                Result::Ok(_) => {
                    debug!("Found branch {attempt} in repository");
                    found = Some(attempt);
                    break;
                }
                Result::Err(_) => {
                    debug!("Branch {attempt} not found in repository");
                }
            }
        }

        found
            .map(|v| v.to_string())
            .ok_or(anyhow::anyhow!("Could not determine mainline branch"))
    }

    pub fn reset_hard(&self, revstring: &str) -> Result<()> {
        let git2_repo: Repository = self.into();

        let (object, _) = git2_repo.revparse_ext(&revstring)?;
        git2_repo.checkout_tree(&object, None)?;
        git2_repo.set_head_detached(object.id())?;

        Ok(())
    }

    pub fn get_all_commits(&self) -> Result<Vec<CommitInfo>> {
        let git2_repo: Repository = self.into();

        let mut revwalk = git2_repo.revwalk().unwrap();

        revwalk.set_sorting(Sort::NONE)?;
        revwalk.push_head()?;

        let mut commits: Vec<_> = Vec::new();
        for id in revwalk {
            let oid = id?;
            let commit = git2_repo.find_commit(oid)?;
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
}

pub fn clone_repository(url: &str, directory: &PathBuf) -> Result<RepositoryHandle> {
    let mut command = Command::new(GIT_BINARY_PATH);
    command.arg("clone");
    command.arg(url);
    command.arg(directory);
    command.execute_check_exit_status_code(0)?;

    Ok(RepositoryHandle {
        path: directory.clone(),
    })
}
