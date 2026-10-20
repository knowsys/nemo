//! A temporary directory for one test's files.

use std::{
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicUsize, Ordering},
};

/// A fresh directory for one test's files, removed with its contents when dropped.
pub(crate) struct TempDirectory(PathBuf);

impl TempDirectory {
    /// Creates an empty directory that no other test uses,
    /// in this process or in another one.
    pub(crate) fn new() -> Self {
        static CREATED: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "nemo-test-{}-{}",
            std::process::id(),
            CREATED.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).expect("the temporary directory is created");
        Self(path)
    }

    /// Returns the directory's path.
    pub(crate) fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
