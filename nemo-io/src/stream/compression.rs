//! Stream compression, either declared or detected from the resource's name or bytes.
//!
//! Compression is independent of the format of the data in the stream.

use std::{
    fs::File,
    io::{self, BufWriter, IntoInnerError, Read, Write},
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
    /// Compression detected from the resource.
    ///
    /// - A resource known by name, such as a file, is gzip if its name ends in `.gz`,
    ///   ignoring the query and fragment of a URL.
    /// - Bytes already in memory are gzip if they start with the gzip magic bytes.
    ///
    /// Bytes are judged by their content, since a fetched body arrives decompressed
    /// if the transport compressed it, whatever its URL ends in.
    #[default]
    Auto,
    /// A gzip stream, possibly of several members.
    ///
    /// A stream without a gzip header is an error, and is never read as uncompressed.
    Gzip,
    /// An uncompressed stream, whatever the name suggests.
    Plain,
}

impl Compression {
    /// Returns the compression with the given name:
    /// [Gzip](Self::Gzip) for `gzip`, [Plain](Self::Plain) for `none`,
    /// and `None` for any other name.
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "gzip" => Some(Compression::Gzip),
            "none" => Some(Compression::Plain),
            _ => None,
        }
    }

    /// Returns whether a stream with the given resource name is read or written through gzip.
    pub fn applies(self, name: &Path) -> bool {
        match self {
            Compression::Gzip => true,
            Compression::Plain => false,
            Compression::Auto => Path::new(resource_path(&name.to_string_lossy()))
                .extension()
                .is_some_and(|extension| extension == "gz"),
        }
    }

    /// Returns whether bytes already in memory are read through gzip.
    ///
    /// Under [Auto](Self::Auto), they are if they start with the gzip magic bytes.
    pub(crate) fn applies_to_bytes(self, bytes: &[u8]) -> bool {
        match self {
            Compression::Gzip => true,
            Compression::Plain => false,
            Compression::Auto => bytes.starts_with(&GZIP_MAGIC),
        }
    }

    /// Returns a resource name without its compression suffix,
    /// and a URL also without its query and fragment.
    ///
    /// The result ends with the extension that names the resource's format.
    pub fn strip_suffix(name: &str) -> &str {
        let path = resource_path(name);
        path.strip_suffix(".gz").unwrap_or(path)
    }

    /// Opens bytes in memory as a resource stream,
    /// decompressing them if gzip is declared,
    /// or under [Auto](Self::Auto) if the bytes start with the gzip magic.
    ///
    /// This is the counterpart of opening a file, for bytes held in memory or delivered by a
    /// fetch.
    /// `name` is the resource's name, used only for a debug message
    /// if the name and the content disagree on gzip.
    pub fn open_bytes(self, bytes: Arc<[u8]>, name: &str) -> Box<dyn Read> {
        let gzip = self.applies_to_bytes(&bytes);
        if self == Compression::Auto && gzip != self.applies(Path::new(name)) {
            tracing::debug!(
                name,
                gzip,
                "the name and the content disagree on gzip; the content decides"
            );
        }
        let cursor = io::Cursor::new(bytes);
        if gzip {
            Box::new(MultiGzDecoder::new(cursor))
        } else {
            Box::new(cursor)
        }
    }

    /// Wraps a target for writing,
    /// compressing through gzip if this compression [applies](Self::applies) to the target's name.
    ///
    /// A target without a name is compressed only under [Gzip](Self::Gzip).
    /// The returned stream must be ended with [finish](CompressedWriter::finish).
    pub(crate) fn wrap<W: Write>(self, target: W, name: Option<&Path>) -> CompressedWriter<W> {
        let gzip = match name {
            Some(name) => self.applies(name),
            None => self == Compression::Gzip,
        };
        let encoder = if gzip {
            Encoder::Gzip(GzEncoder::new(target, flate2::Compression::default()))
        } else {
            Encoder::Plain(target)
        };
        CompressedWriter {
            buffer: BufWriter::new(encoder),
        }
    }
}

/// A byte stream that is written to a target through a buffer, and compressed if its
/// [Compression] applies.
///
/// The buffer collects small writes into large ones,
/// so that the target is not written once per term or per cell.
/// It sits in front of the encoder, so that gzip compresses large blocks too.
/// The stream must be ended with [finish](Self::finish).
/// A stream that is dropped instead writes its last bytes without reporting an error.
#[derive(Debug)]
pub(crate) struct CompressedWriter<W: Write> {
    /// The buffer in front of the encoder.
    buffer: BufWriter<Encoder<W>>,
}

impl<W: Write> CompressedWriter<W> {
    /// Ends the stream: writes the buffered bytes, ends the gzip stream if there is one,
    /// and flushes the target.
    ///
    /// # Errors
    ///
    /// Returns the error of a write to the target.
    pub(crate) fn finish(self) -> io::Result<()> {
        let encoder = self
            .buffer
            .into_inner()
            .map_err(IntoInnerError::into_error)?;
        let mut target = match encoder {
            Encoder::Plain(target) => target,
            Encoder::Gzip(encoder) => encoder.finish()?,
        };
        target.flush()
    }
}

impl<W: Write> Write for CompressedWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.buffer.write(bytes)
    }

    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.buffer.write_all(bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.buffer.flush()
    }
}

/// The encoder between a [CompressedWriter]'s buffer and its target.
#[derive(Debug)]
enum Encoder<W: Write> {
    /// Passes the bytes on unchanged.
    Plain(W),
    /// Compresses the bytes into a gzip stream.
    Gzip(GzEncoder<W>),
}

impl<W: Write> Write for Encoder<W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        match self {
            Encoder::Plain(target) => target.write(bytes),
            Encoder::Gzip(encoder) => encoder.write(bytes),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Encoder::Plain(target) => target.flush(),
            Encoder::Gzip(encoder) => encoder.flush(),
        }
    }
}

/// Returns the part of a resource name that its extensions are read from.
///
/// That is a URL without its query and fragment, or any other name whole.
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Under `Auto`, a name ending in `.gz` is read through gzip,
    /// and a declared compression applies whatever the name.
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

    /// A URL's query and fragment are not part of the name its extensions are read from,
    /// while a local file name keeps every character.
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
        // A local file name may contain the characters a URL reserves.
        assert!(Compression::Auto.applies(Path::new("data#1.csv.gz")));
        assert_eq!(Compression::strip_suffix("data#1.csv.gz"), "data#1.csv");
    }

    /// Under `Auto`, bytes in memory are read through gzip if they start with its magic bytes,
    /// whatever their name, and a declared compression is strict.
    #[test]
    fn in_memory_bytes_are_read_by_their_content() {
        let mut compressed = Vec::new();
        let mut stream = Compression::Gzip.wrap(&mut compressed, None);
        stream.write_all(b"in memory").expect("writes");
        stream.finish().expect("finishes");
        let read = |bytes: &[u8], name: &str, compression: Compression| {
            let mut text = String::new();
            compression
                .open_bytes(Arc::from(bytes), name)
                .read_to_string(&mut text)
                .map(|_| text)
        };
        // Gzip bytes are decompressed whatever the name says. Plain bytes under a
        // `.gz` name, such as a body the transport decompressed, are read as they are.
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

    /// A file written through gzip, as its name asks, is read back.
    #[test]
    fn written_streams_read_back() {
        let directory = crate::testing::TempDirectory::new();
        let path = directory.path().join("text.txt.gz");
        let file = File::create(&path).expect("the file creates");
        let mut stream = Compression::Auto.wrap(file, Some(&path));
        stream.write_all(b"hello").expect("the bytes write");
        stream.finish().expect("the stream ends");
        let mut text = String::new();
        open(&path, Compression::Auto)
            .expect("the file opens")
            .read_to_string(&mut text)
            .expect("the bytes read");
        assert_eq!(text, "hello");
    }
}
