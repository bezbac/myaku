use std::{
    collections::HashSet,
    env::temp_dir,
    fmt::Formatter,
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

use chrono::{offset::LocalResult, DateTime, TimeZone, Utc};
use execute::Execute;
use git2::{Diff, DiffFormat, DiffOptions, Object, ObjectType, Oid, Repository, Signature, Sort};
use rand::{distributions::Alphanumeric, Rng};
use regex::Regex;
use serde::{Deserialize, Serialize, Serializer};
use ssh_key::{LineEnding, PrivateKey};
use thiserror::Error;
use tracing::{debug, warn};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Author {
    pub name: Option<String>,
    pub email: Option<String>,
}

#[derive(PartialEq, Eq, Hash, Clone, Serialize, Deserialize, Debug)]
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

fn serialize_time<S>(x: &DateTime<Utc>, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    s.serialize_i64(x.timestamp())
}

fn deserialize_time<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let timestamp = i64::deserialize(deserializer)?;
    Ok(Utc.timestamp_opt(timestamp, 0).unwrap())
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CommitInfo {
    pub id: CommitHash,
    pub author: Author,
    pub committer: Author,
    pub message: Option<String>,
    #[serde(
        serialize_with = "serialize_time",
        deserialize_with = "deserialize_time"
    )]
    pub time: DateTime<Utc>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CommitTagInfo {
    pub name: String,
    pub commit: CommitHash,
}

impl From<Signature<'_>> for Author {
    fn from(item: Signature) -> Self {
        Author {
            name: item.name().map(|v| v.to_string()),
            email: item.email().map(|v| v.to_string()),
        }
    }
}

const GIT_BINARY_PATH: &str = "git";

#[derive(Debug)]
pub struct WorktreeHandle<'r> {
    repo: &'r RepositoryHandle,
    pub name: String,
    pub path: PathBuf,
}

pub struct TempWorktreeHandle<'r> {
    worktree: WorktreeHandle<'r>,
}

impl Drop for TempWorktreeHandle<'_> {
    fn drop(&mut self) {
        let res = self
            .worktree
            .repo
            .remove_worktree(&self.worktree.name, Some(true));

        if res.is_err() {
            warn!(
                "Failed to remove temporary worktree: {}",
                self.worktree.name
            );
        }
    }
}

impl<'r> AsRef<WorktreeHandle<'r>> for TempWorktreeHandle<'r> {
    fn as_ref(&self) -> &WorktreeHandle<'r> {
        &self.worktree
    }
}

impl<'r> AsMut<WorktreeHandle<'r>> for TempWorktreeHandle<'r> {
    fn as_mut(&mut self) -> &mut WorktreeHandle<'r> {
        &mut self.worktree
    }
}

#[derive(Debug)]
pub struct RepositoryHandle {
    pub path: PathBuf,
}

impl TryFrom<&RepositoryHandle> for Repository {
    type Error = git2::Error;

    fn try_from(value: &RepositoryHandle) -> Result<Self, Self::Error> {
        Repository::open(&value.path)
    }
}

#[derive(Error, Debug)]
pub enum GitError {
    #[error(".git directory does not exist in path {0}")]
    NoGitDirectory(PathBuf),

    #[error("Could not determine remote URL")]
    FailedToDetermineRemoteURL,

    #[error("Could not determine mainline branch")]
    FailedToDetermineMainlineBranch,

    #[error("Failed to create git object")]
    FailedToGetGitObject,

    #[error("Failed to convert git object time: {time:?}")]
    FailedToConvertGitObjectTime { time: git2::Time },

    #[error("Unexpected tag name format: {tag_name}")]
    UnexpectedTagNameFormat { tag_name: String },

    #[error("{0}")]
    CloneError(#[from] GitCloneError),

    #[error("IO error: {0}")]
    IO(#[from] std::io::Error),

    #[error("Could not parse string: {0}")]
    StringParsing(#[from] std::string::FromUtf8Error),

    #[error("Git error: {0}")]
    Git2Erorr(#[from] git2::Error),
}

impl RepositoryHandle {
    pub fn open(path: &Path) -> Result<RepositoryHandle, GitError> {
        if path.join(".git").exists() {
            return Ok(RepositoryHandle {
                path: path.to_path_buf(),
            });
        }

        Err(GitError::NoGitDirectory(path.to_path_buf()))
    }

    pub fn fetch(&self) -> Result<(), GitError> {
        let mut command = Command::new(GIT_BINARY_PATH);
        command.current_dir(&self.path);
        command.arg("fetch");
        command.execute_check_exit_status_code(0)?;

        Ok(())
    }

    pub fn remote_url(&self) -> Result<String, GitError> {
        let git2_repo: Repository = self.try_into()?;

        let remote = git2_repo.find_remote("origin")?;
        let url = remote.url().ok_or(GitError::FailedToDetermineRemoteURL)?;

        Ok(url.to_string())
    }

    pub fn find_main_branch(&self) -> Result<String, GitError> {
        let git2_repo: Repository = self.try_into()?;

        let mut found = Option::None;
        for attempt in &["master", "main", "dev", "development", "develop"] {
            match git2_repo.find_branch(&format!("origin/{attempt}"), git2::BranchType::Remote) {
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
            .map(|v| (*v).to_string())
            .ok_or(GitError::FailedToDetermineMainlineBranch)
    }

    pub fn reset_hard(&self, revstring: &str) -> Result<(), GitError> {
        let main_worktree = self.main_worktree();
        main_worktree.reset_hard(revstring)
    }

    pub fn get_all_commits(&self) -> Result<Vec<CommitInfo>, GitError> {
        let git2_repo: Repository = self.try_into()?;

        let mut revwalk = git2_repo.revwalk()?;

        revwalk.set_sorting(Sort::NONE)?;
        revwalk.push_head()?;

        let mut commits: Vec<_> = Vec::new();
        for id in revwalk {
            let oid = id?;
            let commit = git2_repo.find_commit(oid)?;

            let Some(LocalResult::Single(time)) =
                chrono::FixedOffset::east_opt(commit.time().offset_minutes() * 60)
                    .map(|time| time.timestamp_opt(commit.time().seconds(), 0))
            else {
                return Err(GitError::FailedToConvertGitObjectTime {
                    time: commit.time(),
                });
            };

            commits.push(CommitInfo {
                id: commit.id().to_string().into(),
                author: commit.author().into(),
                committer: commit.committer().into(),
                message: commit.message().map(|v| v.to_string()),
                time: time.to_utc(),
            });
        }

        Ok(commits)
    }

    pub fn get_all_commit_tags(&self) -> Result<Vec<CommitTagInfo>, GitError> {
        let git2_repo: Repository = self.try_into()?;

        let mut tag_ids_and_names: Vec<(Oid, Vec<u8>)> = Vec::new();

        git2_repo.tag_foreach(|tag_id, name| {
            tag_ids_and_names.push((tag_id, name.to_vec()));
            true
        })?;

        let mut tags = Vec::new();

        for (tag_id, tag_name) in tag_ids_and_names {
            let tag_name = String::from_utf8(tag_name)?;
            let tag_name =
                tag_name
                    .strip_prefix("refs/tags/")
                    .ok_or(GitError::UnexpectedTagNameFormat {
                        tag_name: tag_name.clone(),
                    })?;

            let Ok(tag) = git2_repo.find_tag(tag_id) else {
                // The tag id might point to a commit

                let commit = git2_repo.find_commit(tag_id);

                if let Ok(commit) = commit {
                    tags.push(CommitTagInfo {
                        name: tag_name.to_string(),
                        commit: commit.id().into(),
                    });
                }

                continue;
            };

            let commit_id = tag.target_id();
            let Ok(commit) = git2_repo.find_commit(commit_id) else {
                continue;
            };

            tags.push(CommitTagInfo {
                name: tag_name.to_string(),
                commit: commit.id().into(),
            });
        }

        Ok(tags)
    }

    pub fn get_current_total_diff_stat(&self) -> Result<(usize, usize, usize), GitError> {
        let main_worktree = self.main_worktree();
        main_worktree.get_current_total_diff_stat()
    }

    pub fn get_current_changed_file_paths(&self) -> Result<HashSet<String>, GitError> {
        let main_worktree = self.main_worktree();
        main_worktree.get_current_changed_file_paths()
    }

    pub fn create_worktree<'a>(
        &'a self,
        worktree_name: &str,
        worktree_path: &Path,
    ) -> Result<WorktreeHandle<'a>, GitError> {
        let git2_repo: Repository = self.try_into()?;

        git2_repo.worktree(worktree_name, worktree_path, None)?;

        let handle = WorktreeHandle {
            repo: self,
            name: worktree_name.to_string(),
            path: worktree_path.to_path_buf(),
        };

        Ok(handle)
    }

    pub fn main_worktree(&self) -> WorktreeHandle<'_> {
        // TODO: Find the real name here
        let main_worktree_name = String::from("main");

        WorktreeHandle {
            repo: self,
            name: main_worktree_name.clone(),
            path: self.path.clone(),
        }
    }

    pub fn create_temp_worktree<'a>(
        &'a self,
        worktree_name: &str,
        worktree_path: &Path,
    ) -> Result<TempWorktreeHandle<'a>, GitError> {
        let worktree = self.create_worktree(worktree_name, worktree_path)?;
        Ok(TempWorktreeHandle { worktree })
    }

    pub fn remove_worktree(
        &self,
        worktree_name: &str,
        force: Option<bool>,
    ) -> Result<(), GitError> {
        let mut command = Command::new(GIT_BINARY_PATH);
        command.current_dir(&self.path);
        command.arg("worktree");
        command.arg("remove");

        if let Some(true) = force {
            command.arg("-f");
        }

        command.arg(worktree_name);

        command.execute_check_exit_status_code(0)?;

        Ok(())
    }
}

impl TryFrom<&WorktreeHandle<'_>> for Repository {
    type Error = git2::Error;

    fn try_from(value: &WorktreeHandle<'_>) -> Result<Self, Self::Error> {
        Repository::open(&value.path)
    }
}

impl WorktreeHandle<'_> {
    pub fn reset_hard(&self, revstring: &str) -> Result<(), GitError> {
        let git2_repo: Repository = self.try_into()?;

        let (object, _) = git2_repo.revparse_ext(revstring)?;
        git2_repo.checkout_tree(&object, None)?;
        git2_repo.set_head_detached(object.id())?;

        Ok(())
    }

    pub fn get_current_total_diff_stat(&self) -> Result<(usize, usize, usize), GitError> {
        let git2_repo: Repository = self.try_into()?;
        let diff = get_current_diff_to_parent(&git2_repo)?;
        let stats = diff.stats()?;
        Ok((stats.files_changed(), stats.insertions(), stats.deletions()))
    }

    pub fn get_current_changed_file_paths(&self) -> Result<HashSet<String>, GitError> {
        let git2_repo: Repository = self.try_into()?;
        let diff = get_current_diff_to_parent(&git2_repo)?;

        let mut diff_lines = Vec::new();
        diff.print(DiffFormat::NameOnly, |_, _, l| {
            diff_lines.push(l.content().to_vec());
            true
        })?;

        let mut changed_files: HashSet<String> = HashSet::new();
        for l in diff_lines {
            changed_files.insert(String::from_utf8(l)?.trim_end().to_string());
        }

        Ok(changed_files)
    }

    pub fn remove(self) -> Result<(), GitError> {
        self.repo.remove_worktree(&self.name, Some(false))
    }

    pub fn list_files(&self) -> Result<Vec<String>, GitError> {
        let git2_repo: Repository = self.try_into()?;
        let mut files = Vec::new();

        let tree = git2_repo.find_tree(git2_repo.head()?.peel_to_tree()?.id())?;
        tree.walk(git2::TreeWalkMode::PreOrder, |_, entry| {
            files.push(entry.name().map(|e| e.to_string()));
            git2::TreeWalkResult::Ok
        })?;

        let files = files.into_iter().flatten().collect();

        Ok(files)
    }
}

fn get_current_diff_to_parent(repo: &Repository) -> Result<Diff<'_>, GitError> {
    // To diff the first commit in a repository, we need something to diff it against other than it's parent
    // This object is the empty tree. See https://stackoverflow.com/a/40884093 for more details.
    let empty_tree = repo.find_tree(Oid::from_str("4b825dc642cb6eb9a060e54bf8d69288fbee4904")?)?;

    let Some(t1) =
        tree_to_treeish(repo, Some(&"HEAD^".to_string())).unwrap_or(Some(empty_tree.into_object()))
    else {
        return Err(GitError::FailedToGetGitObject);
    };

    let Some(t2) = tree_to_treeish(repo, Some(&"HEAD".to_string()))? else {
        return Err(GitError::FailedToGetGitObject);
    };

    let diff = repo.diff_tree_to_tree(t1.as_tree(), t2.as_tree(), Some(&mut DiffOptions::new()))?;

    Ok(diff)
}

fn tree_to_treeish<'a>(
    repo: &'a Repository,
    arg: Option<&String>,
) -> Result<Option<Object<'a>>, git2::Error> {
    let Some(arg) = arg else {
        return Ok(None);
    };

    let obj = repo.revparse_single(arg)?;
    let tree = obj.peel(ObjectType::Tree)?;
    Ok(Some(tree))
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
    fn try_from(line: &str) -> Result<CloneProgress, GitCloneError> {
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
                .ok_or(GitCloneError::FailedToMatchProgress(line.to_string()))?;

            let (finished, total) = {
                let mut tmp = progress.as_str().split('(');
                tmp.next();

                let Some(tmp) = tmp.next() else {
                    return Err(GitCloneError::FailedToMatchProgress(line.to_string()));
                };

                let tmp = tmp.replace(')', "");
                let mut parts = tmp.split('/');

                let Some(finished) = parts.next() else {
                    return Err(GitCloneError::FailedToMatchProgress(line.to_string()));
                };

                let finished = finished.parse::<usize>()?;

                let Some(total) = parts.next() else {
                    return Err(GitCloneError::FailedToMatchProgress(line.to_string()));
                };

                let total = total.trim().parse::<usize>()?;

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

        Err(GitCloneError::FailedToMatchProgress(line.to_string()))
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

                    if buffer.is_empty() {
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

pub fn create_temp_ssh_key_file(ssh_key: &PrivateKey) -> Result<PathBuf, ssh_key::Error> {
    let filename = format!(
        "{}.key",
        &rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(12)
            .map(char::from)
            .collect::<String>()
    );

    let path = temp_dir().join(filename);

    ssh_key.write_openssh_file(&path, LineEnding::default())?;
    Ok(path)
}

#[derive(Error, Debug)]
pub enum GitCloneError {
    #[error("The command exited with code {0}")]
    NonZeroExitCode(std::process::ExitStatus),

    #[error("IO error: {0}")]
    IO(#[from] std::io::Error),

    #[error("SSH key error: {0}")]
    SSHKey(#[from] ssh_key::Error),

    #[error("Regex error: {0}")]
    Regex(#[from] regex::Error),

    #[error("Failed to match progress from line {0}")]
    FailedToMatchProgress(String),

    #[error("Failed to parse int: {0}")]
    ParseIntError(#[from] std::num::ParseIntError),

    #[error("Failed to process command output line: {0}")]
    FailedToProcessCommandOutput(String),
}

pub fn clone_repository(
    url: &str,
    directory: &PathBuf,
    progress_callback: impl Fn(&CloneProgress),
    ssh_key: Option<&PrivateKey>,
) -> Result<RepositoryHandle, GitCloneError> {
    let mut command = Command::new(GIT_BINARY_PATH);
    command.arg("clone");
    command.arg(url);
    command.arg(directory);
    command.arg("--progress");

    if let Some(private_key) = ssh_key {
        let private_key_file = create_temp_ssh_key_file(private_key)?;
        command.env(
            "GIT_SSH_COMMAND",
            format!(
                "ssh -i {} -o IdentitiesOnly=yes",
                private_key_file.display()
            ),
        );
    }

    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;

    let Some(stdout) = child.stderr.take() else {
        return Err(GitCloneError::FailedToProcessCommandOutput(
            "No stdout available from git clone command".to_string(),
        ));
    };

    let mut lines = vec![];
    let reader: BufReaderWithDelimitedBy<_> = BufReader::new(stdout).into();

    for line in reader.delimited_by(&['\n', '\r']) {
        let Ok(line) = line else {
            warn!("Failed to read line from git clone output: {:?}", line);
            continue;
        };

        if line.trim().is_empty() {
            continue;
        }

        let progress = CloneProgress::try_from(&line);
        if let Ok(progress) = progress {
            progress_callback(&progress);
        }

        lines.push(line);
    }

    let exit = child.wait()?;

    if !exit.success() {
        debug!("{}", lines.join("\n"));
        return Err(GitCloneError::NonZeroExitCode(exit));
    }

    Ok(RepositoryHandle {
        path: directory.clone(),
    })
}
