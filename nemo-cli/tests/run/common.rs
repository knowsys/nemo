//! What the tests share: a directory of each test's own, runs of the binary inside it, and the
//! program the selection tests run.

use std::{
    fs,
    ops::Deref,
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

/// What one run produced.
#[derive(Debug)]
pub(crate) struct Outcome {
    /// The exit code, absent if a signal ended the run.
    pub(crate) status: Option<i32>,
    /// What the run reported on the standard output.
    pub(crate) stdout: String,
    /// What the run reported on the error stream.
    pub(crate) stderr: String,
}

/// A directory of one test's own, removed again when the test ends, however it ends.
pub(crate) struct Workspace(PathBuf);

impl Deref for Workspace {
    type Target = Path;

    fn deref(&self) -> &Path {
        &self.0
    }
}

impl Drop for Workspace {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// A directory of this test's own, emptied if an earlier run left one.
pub(crate) fn workspace(name: &str) -> Workspace {
    let path = std::env::temp_dir().join(format!("nemo-cli-{}-{name}", std::process::id()));
    let _ = fs::remove_dir_all(&path);
    fs::create_dir_all(&path).expect("the temporary directory is creatable");
    Workspace(path)
}

/// Writes a file into a workspace.
pub(crate) fn write(directory: &Path, name: &str, content: &str) {
    fs::write(directory.join(name), content).expect("the workspace is writable");
}

/// Runs the binary inside a workspace, without colours so the reported
/// text is what the assertions compare.
pub(crate) fn run(directory: &Path, arguments: &[&str]) -> Outcome {
    run_with_stdin(directory, arguments, "")
}

/// The command every run starts from.
pub(crate) fn command(directory: &Path, arguments: &[&str]) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_nmo"));
    command
        .args(arguments)
        .env("NO_COLOR", "1")
        .current_dir(directory)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    command
}

/// Runs the binary with the given text on its standard input.
pub(crate) fn run_with_stdin(directory: &Path, arguments: &[&str], stdin: &str) -> Outcome {
    use std::io::Write;

    let mut child = command(directory, arguments)
        .stdin(Stdio::piped())
        .spawn()
        .expect("the binary runs");
    child
        .stdin
        .take()
        .expect("stdin is piped")
        .write_all(stdin.as_bytes())
        .expect("stdin accepts the text");
    let output = child.wait_with_output().expect("the binary finishes");
    Outcome {
        status: output.status.code(),
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    }
}

/// The program every selection test runs: one given predicate, one
/// derived, one both.
pub(crate) const ROLES: &str = "edge(1, 2) . edge(2, 3) .\n\
    reach(?x, ?y) :- edge(?x, ?y) .\n\
    reach(?x, ?z) :- reach(?x, ?y), edge(?y, ?z) .\n\
    both(0) .\n\
    both(?x) :- edge(?x, _) .\n";
