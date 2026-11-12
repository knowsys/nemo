//! Batches of delimiter-separated resources, parsed in parallel and interned in resource order.

use std::{io::Read, path::PathBuf, sync::Arc};

use nemo_physical::{diagnostics::Diagnostic, dictionary::Dictionary, function::EvaluationContext};

use crate::{
    compression::Compression,
    load::{
        Loaded, Resource,
        batch::{self, Cuts, RunSource, WINDOW_BYTES},
        run::RunPool,
    },
};

use super::{
    ColumnLayout, DsvReader, DsvReaderOptions,
    record_ends::QuotedRecordEnds,
    run::{DsvLoad, DsvRun},
};

/// One delimiter-separated resource of a batch.
#[derive(Debug, Clone)]
pub struct DsvResource {
    /// Where the bytes come from.
    pub input: Resource,
    /// The options of the format, such as the delimiter.
    pub options: DsvReaderOptions,
}

impl DsvResource {
    /// Creates a resource that reads the given file.
    pub fn path(path: impl Into<PathBuf>, options: DsvReaderOptions) -> Self {
        Self {
            input: Resource::Path(path.into()),
            options,
        }
    }

    /// Creates a resource that reads bytes already in memory, under `name` in diagnostics.
    pub fn bytes(
        name: impl Into<String>,
        bytes: impl Into<Arc<[u8]>>,
        options: DsvReaderOptions,
    ) -> Self {
        Self {
            input: Resource::Bytes {
                name: name.into(),
                bytes: bytes.into(),
            },
            options,
        }
    }
}

impl RunSource for DsvResource {
    type Run = DsvRun;
    type Preface = Option<ColumnLayout>;
    type Load<'a> = DsvLoad<'a>;

    fn input(&self) -> &Resource {
        &self.input
    }

    fn compression(&self) -> Compression {
        self.options.compression
    }

    /// A resource is not cut under a row limit, which counts rows in the resource's order
    /// across all windows.
    /// Under quoting, a line end inside a quoted field does not end a record,
    /// so the resource is cut only where a scan from its start finds a record to end.
    fn cuts(&self) -> Cuts {
        if self.options.limit.is_some() {
            Cuts::Nowhere
        } else if self.options.quoting {
            Cuts::RowEnds(Box::new(QuotedRecordEnds::new(self.options.delimiter)))
        } else {
            Cuts::Lines
        }
    }

    /// Returns where the records have the cells of the named columns, read from the header,
    /// or `None` for a resource whose columns go by position.
    fn preface(&self) -> Result<Option<ColumnLayout>, Diagnostic> {
        if self.options.columns.is_none() {
            return Ok(None);
        }

        let source = batch::open(&self.input, self.options.compression)?;

        DsvReader::new(self.options.clone()).header_layout(source)
    }

    fn parse(
        &self,
        source: impl Read,
        first: bool,
        layout: &Option<ColumnLayout>,
        runs: &RunPool<DsvRun>,
        context: EvaluationContext,
        emit: &mut dyn FnMut(DsvRun) -> bool,
    ) -> Result<(), Diagnostic> {
        let mut options = self.options.clone();

        // Only the first window of a resource contains the header to skip.
        options.ignore_headers = (self.options.ignore_headers || layout.is_some()) && first;

        DsvReader::new(options).parse(source, runs, context, layout.as_ref(), emit)
    }

    fn run_bytes(run: &DsvRun) -> usize {
        run.bytes()
    }

    fn consumed_lines(run: &DsvRun) -> u64 {
        run.consumed_lines()
    }

    fn start(&self, _dictionary: &mut Dictionary, context: EvaluationContext) -> DsvLoad<'_> {
        DsvLoad::new(&self.options, context)
    }

    fn intern(run: &DsvRun, load: &mut DsvLoad<'_>, dictionary: &mut Dictionary, base: u64) {
        run.encode_into(load, dictionary, base);
    }

    fn finish(load: DsvLoad<'_>, dictionary: &mut Dictionary) -> Loaded {
        load.finish(dictionary)
    }
}

/// A batch of delimiter-separated resources loaded together.
///
/// The resources are parsed in parallel, window by window.
/// A large resource that is uncompressed and without a row limit is cut into windows
/// at record boundaries, so that one bulk resource keeps every worker busy.
/// A resource with quoted fields is read once more before that, to find the line ends that
/// end a record.
/// Each window hands its rows on in runs of bounded size,
/// and the calling thread interns the runs into the dictionary as they arrive,
/// in the order of the resources and their windows.
/// So the dictionary has one writer,
/// and receives the values in the order that a sequential read would give them.
///
/// A worker's parse waits once the runs it holds ahead of the interning reach a bound,
/// so a batch keeps at most that much in memory per worker, however large its resources.
/// A batch of one small window is parsed on the calling thread,
/// which interns each run as it is parsed.
#[derive(Debug)]
pub struct DsvBatch {
    /// The resources, loaded in order.
    sources: Vec<DsvResource>,
    /// The size a window aims for, which is [WINDOW_BYTES] unless a test changes it.
    window_bytes: u64,
}

impl DsvBatch {
    /// Creates a batch over the given resources.
    pub fn new(sources: Vec<DsvResource>) -> Self {
        Self {
            sources,
            window_bytes: WINDOW_BYTES,
        }
    }

    /// Returns the number of windows that the resource at `index` is cut into.
    #[cfg(test)]
    fn windows(&self, index: usize) -> usize {
        batch::windows(&self.sources[index], index, self.window_bytes).len()
    }

    /// Loads every resource, and returns the loads in the order of the batch.
    ///
    /// `context` is the evaluation context of the resources' conditions.
    ///
    /// # Errors
    ///
    /// Returns the first failure to read a resource.
    /// A resource with named columns fails if its header lacks one of the names,
    /// or has it more than once.
    pub fn load(
        &self,
        dictionary: &mut Dictionary,
        context: EvaluationContext,
    ) -> Result<Vec<Loaded>, Diagnostic> {
        batch::load(&self.sources, self.window_bytes, dictionary, context)
    }
}

#[cfg(test)]
mod tests;
