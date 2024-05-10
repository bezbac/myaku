use std::{
    collections::HashSet,
    fmt::Formatter,
    io::{BufRead, BufReader},
    path::PathBuf,
    process::{Command, Stdio},
};

use anyhow::Result;
use execute::Execute;
use git2::{Oid, Repository, Signature, Sort};
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

impl From<Oid> for CommitHash {
    fn from(item: Oid) -> Self {
        CommitHash(item.to_string())
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

#[derive(Serialize, Deserialize, Debug)]
pub struct CommitTagInfo {
    pub name: String,
    pub commit: CommitHash,
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

    pub fn remote_url(&self) -> Result<String> {
        let git2_repo: Repository = self.into();

        let remote = git2_repo.find_remote("origin")?;
        let url = remote
            .url()
            .ok_or(anyhow::anyhow!("Could not determine remote URL"))?;

        Ok(url.to_string())
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

    pub fn get_all_commit_tags(&self) -> Result<Vec<CommitTagInfo>> {
        let git2_repo: Repository = self.into();

        let mut tag_ids_and_names: Vec<(Oid, Vec<u8>)> = Vec::new();

        git2_repo.tag_foreach(|tag_id, name| {
            tag_ids_and_names.push((tag_id, name.to_vec()));
            true
        })?;

        let mut tags = Vec::new();

        for (tag_id, tag_name) in tag_ids_and_names {
            let tag_name = String::from_utf8(tag_name)?;
            let tag_name = tag_name.strip_prefix("refs/tags/").ok_or(anyhow::anyhow!(
                "Tag name has an unexpected format: {}",
                tag_name
            ))?;

            let tag = match git2_repo.find_tag(tag_id) {
                Ok(tag) => tag,
                Err(_) => {
                    // The tag id might point to a commit

                    let commit = git2_repo.find_commit(tag_id);

                    if let Some(commit) = commit.ok() {
                        tags.push(CommitTagInfo {
                            name: tag_name.to_string(),
                            commit: commit.id().into(),
                        });
                    }

                    continue;
                }
            };

            let commit_id = tag.target_id();

            let commit = match git2_repo.find_commit(commit_id) {
                Ok(commit) => commit,
                Err(_) => continue,
            };

            tags.push(CommitTagInfo {
                name: tag_name.to_string(),
                commit: commit.id().into(),
            });
        }

        Ok(tags)
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

struct DelimitedBy<R> {
    reader: BufReader<R>,
    delimiters: HashSet<u8>,
}

fn delimited_by<R>(f: BufReader<R>, delimiters: &[char]) -> DelimitedBy<R> {
    DelimitedBy {
        reader: f,
        delimiters: delimiters.iter().map(|v| *v as u8).collect(),
    }
}

impl<R: std::io::Read> Iterator for DelimitedBy<R> {
    type Item = std::io::Result<String>;

    fn next(&mut self) -> Option<Self::Item> {
        let (string, length) = {
            match self.reader.fill_buf() {
                Ok(buffer) => {
                    let line_size = buffer
                        .iter()
                        .take_while(|c| !self.delimiters.contains(*c))
                        .count();

                    if buffer.len() == 0 {
                        return None;
                    }

                    let string = String::from_utf8_lossy(&buffer[..line_size]);

                    // Add count of trailing delimiters to length, so that they are also consumed
                    let mut length = line_size;
                    if line_size < buffer.len() {
                        let mut i = 0;
                        while i < buffer.len() {
                            if !self.delimiters.contains(&buffer[i]) {
                                break;
                            }

                            i += 1;
                        }

                        length += i;
                    }

                    (string.to_string(), length)
                }
                Err(e) => return Some(Err(e)),
            }
        };

        self.reader.consume(length);

        Some(Ok(string))
    }
}

struct BufReaderWithDelimitedBy<R>(BufReader<R>);

impl<R> BufReaderWithDelimitedBy<R> {
    fn delimited_by(self, delimiters: &[char]) -> DelimitedBy<R> {
        delimited_by(self.0, delimiters)
    }
}

impl<R> From<BufReader<R>> for BufReaderWithDelimitedBy<R> {
    fn from(value: BufReader<R>) -> Self {
        BufReaderWithDelimitedBy(value)
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

    let reader: BufReaderWithDelimitedBy<_> = BufReader::new(stdout).into();

    reader.delimited_by(&['\n', '\r']).for_each(|line| {
        let line = line.unwrap();
        let progress = CloneProgress::try_from(&line);
        if let Ok(progress) = progress {
            progress_callback(&progress);
        }
    });

    Ok(RepositoryHandle {
        path: directory.clone(),
    })
}
