use std::{
    fmt::Formatter,
    io::{BufRead, BufReader},
    path::PathBuf,
    process::{Command, Stdio},
};

use anyhow::Result;
use execute::Execute;
use git2::{Repository, Signature, Sort};
use log::debug;
use regex::Regex;
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

#[derive(PartialEq, Debug)]
pub enum CloneProgress {
    EnumeratingObjects,
    CountingObjects { finished: usize, total: usize },
    CompressingObjects { finished: usize, total: usize },
    ReceivingObjects { finished: usize, total: usize },
    ResolvingDeltas { finished: usize, total: usize },
}

impl CloneProgress {
    fn try_from(line: &str) -> Result<CloneProgress> {
        if line.starts_with("Enumerating objects:") {
            return Ok(CloneProgress::EnumeratingObjects);
        }

        if line.starts_with("Counting objects:")
            || line.starts_with("Compressing objects:")
            || line.starts_with("Receiving objects:")
            || line.starts_with("Resolving deltas:")
        {
            let re = Regex::new(r"%\s\((\d+\/\d+)\)")?;

            let progress = re
                .find(line)
                .ok_or(anyhow::anyhow!("Could not find progress in line"))?;

            let (finished, total) = {
                let mut tmp = progress.as_str().split("(");
                tmp.next();
                let tmp = tmp.next().unwrap().replace(")", "");
                let mut parts = tmp.split("/");
                let finished = parts.next().unwrap().parse::<usize>()?;
                let total = parts.next().unwrap().parse::<usize>()?;
                (finished, total)
            };

            if line.starts_with("Counting objects:") {
                return Ok(CloneProgress::CountingObjects { finished, total });
            }

            if line.starts_with("Compressing objects:") {
                return Ok(CloneProgress::CompressingObjects { finished, total });
            }

            if line.starts_with("Receiving objects:") {
                return Ok(CloneProgress::ReceivingObjects { finished, total });
            }

            if line.starts_with("Resolving deltas:") {
                return Ok(CloneProgress::ResolvingDeltas { finished, total });
            }
        }

        return Err(anyhow::anyhow!(
            "Could not parse git progress from line: {}",
            line
        ));
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case("Enumerating objects: 2341, done.", CloneProgress::EnumeratingObjects)]
    #[case("Counting objects:   1% (4/336)", CloneProgress::CountingObjects { finished: 4, total: 336 })]
    #[case("Compressing objects:   1% (2/141)", CloneProgress::CompressingObjects { finished: 2, total: 141 })]
    #[case("Receiving objects:   1% (24/2341)", CloneProgress::ReceivingObjects { finished: 24, total: 2341 })]
    #[case("Resolving deltas:   1% (14/1203)", CloneProgress::ResolvingDeltas { finished: 14, total: 1203 })]
    fn test_clone_progress_from_line(#[case] input: &str, #[case] expected: CloneProgress) {
        assert_eq!(expected, CloneProgress::try_from(input).unwrap());
    }
}

pub fn clone_repository(
    url: &str,
    directory: &PathBuf,
    progress_callback: impl Fn(&CloneProgress) -> (),
) -> Result<RepositoryHandle> {
    let mut command = Command::new(GIT_BINARY_PATH);
    command.arg("clone");
    command.arg(url);
    command.arg(directory);
    command.arg("--progress");

    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;

    let stdout = child.stderr.take().unwrap();

    // Iterator over the strings delimited by `\n` or `\r`
    // https://github.com/rust-lang/rust/issues/55743#issuecomment-436937262
    let mut f = BufReader::new(stdout);
    loop {
        let length = {
            let buffer = f.fill_buf()?;
            let line_size = buffer
                .iter()
                .take_while(|c| **c != b'\n' && **c != b'\r')
                .count();

            if buffer.len() == 0 {
                break;
            }

            let string = String::from_utf8_lossy(&buffer[..line_size]);

            let progress = CloneProgress::try_from(&string);
            match progress {
                Result::Ok(progress) => progress_callback(&progress),
                Result::Err(err) => debug!("{}", err),
            }

            line_size
                + if line_size < buffer.len() {
                    // we found a delimiter
                    if line_size + 1 < buffer.len() // we look if we found two delimiter
                    && buffer[line_size] == b'\r'
                    && buffer[line_size + 1] == b'\n'
                    {
                        2
                    } else {
                        1
                    }
                } else {
                    0
                }
        };

        f.consume(length);
    }

    if !child.wait()?.success() {
        return Err(anyhow::anyhow!("Failed to clone repository"));
    }

    Ok(RepositoryHandle {
        path: directory.clone(),
    })
}
