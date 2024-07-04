use std::{
    collections::HashSet,
    fmt::Formatter,
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

use anyhow::Result;
use chrono::{DateTime, TimeZone, Utc};
use execute::Execute;
use git2::{Diff, DiffFormat, DiffOptions, Object, ObjectType, Oid, Repository, Signature, Sort};
use regex::Regex;
use serde::{Deserialize, Serialize, Serializer};
use tracing::debug;

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

impl<'a> From<Signature<'a>> for Author {
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

impl<'r> Drop for TempWorktreeHandle<'r> {
    fn drop(&mut self) {
        self.worktree
            .repo
            .remove_worktree(&self.worktree.name, Some(true))
            .unwrap();
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

impl From<&RepositoryHandle> for Repository {
    fn from(value: &RepositoryHandle) -> Self {
        Repository::open(&value.path).unwrap()
    }
}

impl RepositoryHandle {
    pub fn open(path: &Path) -> Result<RepositoryHandle> {
        if path.join(".git").exists() {
            return Ok(RepositoryHandle {
                path: path.to_path_buf(),
            });
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
            .ok_or(anyhow::anyhow!("Could not determine mainline branch"))
    }

    pub fn reset_hard(&self, revstring: &str) -> Result<()> {
        let main_worktree = self.main_worktree();
        main_worktree.reset_hard(revstring)
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

            let time = chrono::FixedOffset::east_opt(commit.time().offset_minutes() * 60)
                .unwrap()
                .timestamp_opt(commit.time().seconds(), 0)
                .unwrap();

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

    pub fn get_current_total_diff_stat(&self) -> Result<(usize, usize, usize)> {
        let main_worktree = self.main_worktree();
        main_worktree.get_current_total_diff_stat()
    }

    pub fn get_current_changed_file_paths(&self) -> Result<HashSet<String>> {
        let main_worktree = self.main_worktree();
        main_worktree.get_current_changed_file_paths()
    }

    pub fn create_worktree(
        &self,
        worktree_name: &str,
        worktree_path: &Path,
    ) -> Result<WorktreeHandle> {
        let git2_repo: Repository = self.into();

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
            name: main_worktree_name.to_string(),
            path: self.path.clone(),
        }
    }

    pub fn create_temp_worktree(
        &self,
        worktree_name: &str,
        worktree_path: &Path,
    ) -> Result<TempWorktreeHandle> {
        let worktree = self.create_worktree(worktree_name, worktree_path)?;
        Ok(TempWorktreeHandle { worktree })
    }

    pub fn remove_worktree(&self, worktree_name: &str, force: Option<bool>) -> Result<()> {
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

impl<'r> From<&WorktreeHandle<'r>> for Repository {
    fn from(value: &WorktreeHandle) -> Self {
        Repository::open(&value.path).unwrap()
    }
}

impl<'r> WorktreeHandle<'r> {
    pub fn reset_hard(&self, revstring: &str) -> Result<()> {
        let git2_repo: Repository = self.into();

        let (object, _) = git2_repo.revparse_ext(revstring)?;
        git2_repo.checkout_tree(&object, None)?;
        git2_repo.set_head_detached(object.id())?;

        Ok(())
    }

    pub fn get_current_total_diff_stat(&self) -> Result<(usize, usize, usize)> {
        let git2_repo: Repository = self.into();
        let diff = get_current_diff_to_parent(&git2_repo)?;
        let stats = diff.stats()?;
        Ok((stats.files_changed(), stats.insertions(), stats.deletions()))
    }

    pub fn get_current_changed_file_paths(&self) -> Result<HashSet<String>> {
        let git2_repo: Repository = self.into();
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

    pub fn remove(self) -> Result<()> {
        self.repo.remove_worktree(&self.name, Some(false))
    }
}

fn get_current_diff_to_parent(repo: &Repository) -> Result<Diff<'_>> {
    // To diff the first commit in a repository, we need something to diff it against other than it's parent
    // This object is the empty tree. See https://stackoverflow.com/a/40884093 for more details.
    let empty_tree = repo.find_tree(Oid::from_str("4b825dc642cb6eb9a060e54bf8d69288fbee4904")?)?;

    let t1 =
        tree_to_treeish(repo, Some(&"HEAD^".to_string())).unwrap_or(Some(empty_tree.into_object()));
    let t2 = tree_to_treeish(repo, Some(&"HEAD".to_string()))?;

    let diff = repo.diff_tree_to_tree(
        t1.unwrap().as_tree(),
        t2.unwrap().as_tree(),
        Some(&mut DiffOptions::new()),
    )?;

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
                let mut tmp = progress.as_str().split('(');
                tmp.next();
                let tmp = tmp.next().unwrap().replace(')', "");
                let mut parts = tmp.split('/');
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

        Err(anyhow::anyhow!(
            "Could not parse git progress from line: {}",
            line
        ))
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

pub fn clone_repository(
    url: &str,
    directory: &PathBuf,
    progress_callback: impl Fn(&CloneProgress),
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
