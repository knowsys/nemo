//! Stream compression, declared or detected from the resource name or the bytes.
//!
//! Compression is independent of the format the stream carries.

use std::{
    fs::File,
    io::{Read, Write},
    path::Path,
    sync::Arc,
};

use flate2::{read::MultiGzDecoder, write::GzEncoder};

use nemo_physical::diagnostics::Diagnostic;

use crate::error::IoError;

/// The two bytes every gzip stream starts with.
const GZIP_MAGIC: [u8; 2] = [0x1f, 0x8b];

/// How a resource's byte stream is compressed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Compression {
    /// Detects the compression:
    /// a file by its name, a final `.gz` extension meaning gzip,
    /// and bytes already in memory by their content, the gzip magic.
    /// A fetched body arrives inflated if the transport compressed it, whatever its URL ends in.
    #[default]
    Auto,
    /// A gzip stream, multi-member streams accepted.
    ///
    /// A stream without a gzip header is an error, never read plainly.
    Gzip,
    /// An uncompressed stream, whatever the name suggests.
    Plain,
}

impl Compression {
    /// Returns the compression a name names: `gzip` or `none`.
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "gzip" => Some(Compression::Gzip),
            "none" => Some(Compression::Plain),
            _ => None,
        }
    }

    /// Returns whether a stream of the given resource name reads (or writes) through gzip.
    pub fn applies(self, name: &Path) -> bool {
        match self {
            Compression::Gzip => true,
            Compression::Plain => false,
            Compression::Auto => Path::new(resource_path(&name.to_string_lossy()))
                .extension()
                .is_some_and(|extension| extension == "gz"),
        }
    }

    /// Returns whether bytes already in memory read through gzip.
    ///
    /// Under [Auto](Self::Auto), they do if they start with the gzip magic.
    pub(crate) fn applies_to_bytes(self, bytes: &[u8]) -> bool {
        match self {
            Compression::Gzip => true,
            Compression::Plain => false,
            Compression::Auto => bytes.starts_with(&GZIP_MAGIC),
        }
    }

    /// Returns a resource name without its compression suffix,
    /// the name its format extension is read from.
    pub fn strip_suffix(name: &str) -> &str {
        let path = resource_path(name);
        path.strip_suffix(".gz").unwrap_or(path)
    }

    /// Opens in-memory bytes as a resource stream,
    /// unwrapping gzip if the bytes carry it or the setting says so.
    ///
    /// This is the counterpart of a file path for bytes held in memory or delivered by a fetch.
    pub fn open_bytes(self, bytes: Arc<[u8]>, name: &str) -> Box<dyn Read> {
        let gzip = self.applies_to_bytes(&bytes);
        if self == Compression::Auto && gzip != self.applies(Path::new(name)) {
            tracing::debug!(
                name,
                gzip,
                "the name and the content disagree on gzip; the content decides"
            );
        }
        let cursor = std::io::Cursor::new(bytes);
        if gzip {
            Box::new(MultiGzDecoder::new(cursor))
        } else {
            Box::new(cursor)
        }
    }

    /// Wraps a writing target, through gzip if this compression applies to the target's name.
    ///
    /// A stream without a name to detect by goes through gzip only if gzip is declared.
    pub fn wrap<'a>(self, target: impl Write + 'a, name: Option<&Path>) -> Box<dyn Write + 'a> {
        let gzip = match name {
            Some(name) => self.applies(name),
            None => self == Compression::Gzip,
        };
        if gzip {
            Box::new(GzEncoder::new(target, flate2::Compression::default()))
        } else {
            Box::new(target)
        }
    }
}

/// Returns the part of a resource name its extensions are read from:
/// a URL without its query and fragment, and any other name whole.
fn resource_path(name: &str) -> &str {
    if !crate::stream::remote::Request::is_remote(name) {
        return name;
    }
    let end = name.find(['?', '#']).unwrap_or(name.len());
    &name[..end]
}

/// Opens a file for reading, decompressing it if declared or detected.
pub(crate) fn open(path: &Path, compression: Compression) -> Result<Box<dyn Read>, Diagnostic> {
    let file = File::open(path).map_err(|error| IoError::unreadable(path, &error))?;
    Ok(if compression.applies(path) {
        Box::new(MultiGzDecoder::new(file))
    } else {
        Box::new(file)
    })
}

/// Creates a file for writing, its parent directories included,
/// compressing it if declared or detected.
pub(crate) fn create(path: &Path, compression: Compression) -> Result<Box<dyn Write>, Diagnostic> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|error| IoError::unwritable(path, &error))?;
    }
    let file = File::create(path).map_err(|error| IoError::unwritable(path, &error))?;
    Ok(compression.wrap(file, Some(path)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detection_reads_the_name() {
        assert!(Compression::Auto.applies(Path::new("data.csv.gz")));
        assert!(!Compression::Auto.applies(Path::new("data.csv")));
        assert!(Compression::Gzip.applies(Path::new("data.csv")));
        assert!(!Compression::Plain.applies(Path::new("data.csv.gz")));
        assert_eq!(Compression::strip_suffix("graph.ttl.gz"), "graph.ttl");
        assert_eq!(Compression::strip_suffix("graph.ttl"), "graph.ttl");
        assert_eq!(Compression::from_name("gzip"), Some(Compression::Gzip));
        assert_eq!(Compression::from_name("zip"), None);
    }

    #[test]
    fn a_urls_query_and_fragment_are_not_part_of_its_name() {
        for url in [
            "https://h/x.csv.gz?dl=1",
            "https://h/x.csv.gz#frag",
            "https://h/x.csv.gz?a=b.c#d.e",
        ] {
            assert!(Compression::Auto.applies(Path::new(url)), "{url}");
            assert_eq!(Compression::strip_suffix(url), "https://h/x.csv", "{url}");
        }
        assert!(!Compression::Auto.applies(Path::new("https://h/x.csv?dl=1.gz")));
        // A local file name may carry the characters a URL reserves.
        assert!(Compression::Auto.applies(Path::new("data#1.csv.gz")));
        assert_eq!(Compression::strip_suffix("data#1.csv.gz"), "data#1.csv");
    }

    #[test]
    fn in_memory_bytes_are_read_by_their_content() {
        let mut compressed = Vec::new();
        Compression::Gzip
            .wrap(&mut compressed, None)
            .write_all(b"in memory")
            .expect("writes");
        let read = |bytes: &[u8], name: &str, compression: Compression| {
            let mut text = String::new();
            compression
                .open_bytes(Arc::from(bytes), name)
                .read_to_string(&mut text)
                .map(|_| text)
        };
        // Gzip bytes decode whatever the name says. Plain bytes under a
        // `.gz` name (a body the transport inflated) read plainly.
        assert_eq!(
            read(&compressed, "x.gz", Compression::Auto).expect("reads"),
            "in memory"
        );
        assert_eq!(
            read(&compressed, "x.csv", Compression::Auto).expect("reads"),
            "in memory"
        );
        assert_eq!(
            read(b"plain", "x.csv.gz", Compression::Auto).expect("reads"),
            "plain"
        );
        assert!(Compression::Auto.applies_to_bytes(&compressed));
        assert!(!Compression::Auto.applies_to_bytes(b"plain"));
        // The declared settings stay strict.
        assert!(read(b"plain", "x.gz", Compression::Gzip).is_err());
        let mut raw = Vec::new();
        Compression::Plain
            .open_bytes(Arc::from(compressed.as_slice()), "x.gz")
            .read_to_end(&mut raw)
            .expect("reads");
        assert_eq!(raw, compressed);
    }

    #[test]
    fn written_streams_read_back() {
        let path =
            std::env::temp_dir().join(format!("nemo-io-compression-{}.txt.gz", std::process::id()));
        create(&path, Compression::Auto)
            .expect("the file creates")
            .write_all(b"hello")
            .expect("the bytes write");
        let mut text = String::new();
        open(&path, Compression::Auto)
            .expect("the file opens")
            .read_to_string(&mut text)
            .expect("the bytes read");
        std::fs::remove_file(&path).ok();
        assert_eq!(text, "hello");
    }
}
