//! Batches of RDF resources, parsed in parallel and interned in resource order.

use std::{io::Read, path::PathBuf, sync::Arc};

use oxrdfio::RdfParser;

use nemo_physical::{diagnostics::Diagnostic, dictionary::Dictionary, function::EvaluationContext};

use crate::{
    compression::Compression,
    load::{
        Loaded, Resource,
        batch::{self, RunSource, WINDOW_BYTES},
        run::{Run, RunPool},
    },
};

use super::{RdfReader, reader::RdfLoad};

/// One RDF resource of a batch.
#[derive(Debug, Clone)]
pub struct RdfResource {
    /// Where the bytes come from.
    pub input: Resource,
    /// The reader of the resource's statements, with its options and the scope of its blank
    /// nodes.
    pub reader: RdfReader,
}

impl RdfResource {
    /// Creates a resource that reads the given file.
    pub fn path(path: impl Into<PathBuf>, reader: RdfReader) -> Self {
        Self {
            input: Resource::Path(path.into()),
            reader,
        }
    }

    /// Creates a resource that reads bytes already in memory, under `name` in diagnostics.
    pub fn bytes(name: impl Into<String>, bytes: impl Into<Arc<[u8]>>, reader: RdfReader) -> Self {
        Self {
            input: Resource::Bytes {
                name: name.into(),
                bytes: bytes.into(),
            },
            reader,
        }
    }
}

impl RunSource for RdfResource {
    type Run = Run;
    type Preface = RdfParser;
    type Load<'a> = RdfLoad;

    fn input(&self) -> &Resource {
        &self.input
    }

    fn compression(&self) -> Compression {
        self.reader.options().compression
    }

    /// Only a line-based variant is cut: each of its lines is read on its own,
    /// while a prefix or a base of another variant holds for the statements after it.
    /// A resource is not cut under a statement limit either,
    /// which counts statements in the resource's order across all windows.
    fn cuts_at_lines(&self) -> bool {
        let options = self.reader.options();

        options.variant.line_based() && options.limit.is_none()
    }

    /// Returns the parser of the resource's variant, which checks the base.
    fn preface(&self) -> Result<RdfParser, Diagnostic> {
        self.reader.parser()
    }

    fn parse(
        &self,
        source: impl Read,
        _first: bool,
        parser: &RdfParser,
        runs: &RunPool<Run>,
        context: EvaluationContext,
        emit: &mut dyn FnMut(Run) -> bool,
    ) -> Result<(), Diagnostic> {
        self.reader
            .parse(parser.clone(), source, runs, context, emit)
    }

    fn run_bytes(run: &Run) -> usize {
        run.bytes()
    }

    fn consumed_lines(run: &Run) -> u64 {
        run.consumed_lines
    }

    fn start(&self, dictionary: &mut Dictionary, context: EvaluationContext) -> RdfLoad {
        self.reader.start(dictionary, context)
    }

    fn intern(run: &Run, load: &mut RdfLoad, dictionary: &mut Dictionary, base: u64) {
        load.intern(run, dictionary, base);
    }

    fn finish(load: RdfLoad, dictionary: &mut Dictionary) -> Loaded {
        load.finish(dictionary)
    }
}

/// A batch of RDF resources loaded together.
///
/// The statements are parsed on worker threads,
/// and the calling thread interns them into the dictionary as they arrive,
/// in the order of the resources and of their statements.
/// So the dictionary has one writer, and a load gives what a [reader](RdfReader) gives:
/// the same rows in the same order, the same nulls for the blank nodes, and the same counts.
///
/// A large N-Triples or N-Quads resource that is uncompressed and without a statement limit
/// is cut into windows at line ends, which are parsed in parallel.
/// Any other resource is parsed on one worker.
/// A worker's parse waits once the statements it holds ahead of the interning reach a bound,
/// so a batch keeps at most that much in memory per worker, however large its resources.
///
/// # Example
///
/// ```
/// use nemo_io::{
///     file_format::RdfVariant,
///     rdf::{RdfBatch, RdfReader, RdfReaderOptions, RdfResource},
/// };
/// use nemo_physical::{dictionary::Dictionary, function::EvaluationContext};
///
/// let triples = "<http://example.org/a> <http://example.org/knows> _:b .\n\
///                _:b <http://example.org/name> \"Bob\" .\n";
/// let reader = RdfReader::new(RdfReaderOptions::new(RdfVariant::NTriples));
/// let batch = RdfBatch::new(vec![RdfResource::bytes("people.nt", triples.as_bytes(), reader)]);
///
/// let mut dictionary = Dictionary::new();
/// let loaded = batch
///     .load(&mut dictionary, EvaluationContext::now())
///     .expect("the triples read");
/// assert_eq!(loaded[0].rows.len(), 2);
/// ```
#[derive(Debug)]
pub struct RdfBatch {
    /// The resources, loaded in order.
    sources: Vec<RdfResource>,
    /// The size a window aims for, which is [WINDOW_BYTES] unless a test changes it.
    window_bytes: u64,
}

impl RdfBatch {
    /// Creates a batch over the given resources.
    pub fn new(sources: Vec<RdfResource>) -> Self {
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
    /// Returns the first failure to read a resource, or the first base that is not an IRI.
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
