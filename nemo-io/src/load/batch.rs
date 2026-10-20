//! Batches of delimiter-separated resources, parsed in parallel and
//! interned in resource order.

use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    ops::ControlFlow,
};

use flate2::read::MultiGzDecoder;

use nemo_physical::{
    diagnostics::Diagnostic,
    dictionary::Dictionary,
    function::EvaluationContext,
    parallel::{self, Tasks},
};

use crate::{
    AboutResource,
    error::IoError,
    formats::dsv::{DsvReader, Parsed, RunPool},
    load::{DsvResource, Loaded, Resource},
    stream::compression,
};

/// The size a window aims for: a resource of at least twice this size splits into windows.
///
/// One bulk file dominating a batch is the common case,
/// and parallelism per resource alone leaves it on one thread.
/// The runs of a window take up to eight times its text (narrow rows of short numbers),
/// which [AHEAD_BYTES] holds whole.
const WINDOW_BYTES: u64 = 2 * 1024 * 1024;

/// The bytes of input a single window must hold before it parses on the worker pool.
///
/// A smaller window parses on the calling thread.
const POOL_BYTES: u64 = 64 * 1024;

/// The bytes of a window's runs a worker may hold ready before interning reaches them.
///
/// A parse that is further ahead waits.
/// Interning takes the windows in order,
/// so a window parses in parallel only as far as its runs fit here.
/// The bound holds the runs of a [window](WINDOW_BYTES) whole.
const AHEAD_BYTES: usize = 16 * 1024 * 1024;

/// The bytes of a whole resource's runs a worker may hold ready before interning reaches them.
///
/// A whole resource has no bound on its size, so its runs cannot be held whole.
/// A couple of runs keep its parse beside its interning.
const WHOLE_AHEAD_BYTES: usize = 4 * 1024 * 1024;

/// One unit of parallel parsing: a resource, or a byte window of one.
#[derive(Debug)]
struct Window {
    /// The resource this window reads.
    source: usize,
    /// The window within the resource's windows, in file order.
    position: usize,
    /// The byte range, `None` for the whole file (through the ordinary
    /// open path, compression included).
    range: Option<std::ops::Range<u64>>,
}

/// A batch of resources loaded together.
///
/// Parsing and decoding run in parallel, window by window.
/// A large unquoted plain file splits into byte windows at record boundaries,
/// so one bulk resource keeps every worker busy.
/// Each window hands its rows on in runs of bounded size,
/// and the calling thread interns the runs into the dictionary as they arrive,
/// in resource and window order.
/// The dictionary so keeps one writer,
/// and holds the terms in the order a sequential read would give them.
/// A worker's parse waits once the runs it holds ahead of the interning reach a bound,
/// so a batch holds that much per worker, however large its resources.
/// The bound holds a window whole,
/// so the workers parse the windows after the one being interned in parallel.
/// A batch of one small window parses on the calling thread, interning each run as it is parsed.
#[derive(Debug)]
pub struct ResourceBatch {
    /// The resources, loaded in order.
    sources: Vec<DsvResource>,
    /// The window size, whose default suits bulk files.
    window_bytes: u64,
}

impl ResourceBatch {
    /// Creates a batch over the given resources.
    pub fn new(sources: Vec<DsvResource>) -> Self {
        Self {
            sources,
            window_bytes: WINDOW_BYTES,
        }
    }

    /// Returns the windows of one resource: byte ranges cut at record boundaries,
    /// or the whole resource if windowing cannot apply.
    ///
    /// It cannot apply under quoting (a record boundary is then not a plain newline),
    /// under compression (no seeking into a stream),
    /// under a row limit (counted in the resource's order,
    /// which windows parsed apart cannot share),
    /// to small resources, or if a scan finds no boundary.
    fn windows(&self, index: usize) -> Vec<Window> {
        let whole = vec![Window {
            source: index,
            position: 0,
            range: None,
        }];
        let source = &self.sources[index];
        if source.options.quoting
            || source.options.limit.is_some()
            || source.input.compressed(source.options.compression)
        {
            return whole;
        }
        let size = match &source.input {
            Resource::Path(path) => match std::fs::metadata(path) {
                Ok(data) => data.len(),
                // The parse will report the unreadable file itself.
                Err(_) => return whole,
            },
            Resource::Bytes { bytes, .. } => bytes.len() as u64,
        };
        if size < self.window_bytes * 2 {
            return whole;
        }
        let mut file = match &source.input {
            Resource::Path(path) => match File::open(path) {
                Ok(file) => Some(file),
                Err(_) => return whole,
            },
            Resource::Bytes { .. } => None,
        };

        let count = usize::try_from(size / self.window_bytes)
            .unwrap_or(usize::MAX)
            .max(2);
        let mut bounds = vec![0u64];
        for split in 1..count {
            let target = size * split as u64 / count as u64;
            let boundary = match (&mut file, &source.input) {
                (Some(file), _) => next_record_boundary(file, target, size),
                (None, Resource::Bytes { bytes, .. }) => bytes
                    [usize::try_from(target).expect("bytes are in memory")..]
                    .iter()
                    .position(|&byte| byte == b'\n')
                    .map(|newline| target + newline as u64 + 1)
                    .filter(|&boundary| boundary < size),
                (None, Resource::Path(_)) => unreachable!("paths opened their file above"),
            };
            match boundary {
                Some(boundary) if boundary > *bounds.last().expect("bounds start filled") => {
                    bounds.push(boundary)
                }
                Some(_) => {}
                // No boundary in reach: keep what we have.
                None => break,
            }
        }
        bounds.push(size);
        if bounds.len() <= 2 {
            return whole;
        }
        bounds
            .windows(2)
            .enumerate()
            .map(|(position, pair)| Window {
                source: index,
                position,
                range: Some(pair[0]..pair[1]),
            })
            .collect()
    }

    /// Returns the bytes of input the batch reads:
    /// the sizes of its files as they are stored, compressed or not, and of its bytes in memory.
    fn input_bytes(&self) -> u64 {
        self.sources
            .iter()
            .map(|source| match &source.input {
                // The parse will report an unreadable file itself.
                Resource::Path(path) => std::fs::metadata(path).map_or(0, |data| data.len()),
                Resource::Bytes { bytes, .. } => bytes.len() as u64,
            })
            .sum()
    }

    /// Parses one window, handing its runs to `emit`.
    ///
    /// A failure to read the window is its last item.
    fn parse(
        &self,
        window: &Window,
        runs: &RunPool,
        emit: &mut dyn FnMut(Result<Parsed, Diagnostic>) -> bool,
    ) {
        let source = &self.sources[window.source];
        let mut options = source.options.clone();
        // Only the source's first window carries the header to skip.
        options.ignore_headers = source.options.ignore_headers && window.position == 0;
        let reader = DsvReader::new(options);
        let mut deliver = |run: Parsed| emit(Ok(run));
        let result = match (&source.input, &window.range) {
            (Resource::Path(path), None) => compression::open(path, source.options.compression)
                .and_then(|file| reader.parse(file, runs, &mut deliver)),
            (Resource::Path(path), Some(range)) => File::open(path)
                .and_then(|mut file| file.seek(SeekFrom::Start(range.start)).map(|_| file))
                .map_err(|error| IoError::unreadable(path, error))
                .and_then(|file| {
                    reader.parse(file.take(range.end - range.start), runs, &mut deliver)
                }),
            (Resource::Bytes { bytes, .. }, range) => {
                let slice: &[u8] = match range {
                    None => bytes,
                    // Windowed bytes are never compressed (the windowing
                    // guard), and the bounds came from this buffer.
                    Some(range) => {
                        let start = usize::try_from(range.start).expect("bytes are in memory");
                        let end = usize::try_from(range.end).expect("bytes are in memory");
                        &bytes[start..end]
                    }
                };
                if source.input.compressed(source.options.compression) {
                    reader.parse(MultiGzDecoder::new(slice), runs, &mut deliver)
                } else {
                    reader.parse(slice, runs, &mut deliver)
                }
            }
        };
        if let Err(diagnostic) = result {
            emit(Err(diagnostic.with_resource(source.input.name())));
        }
    }

    /// Loads every resource, delivering the loads in the batch's order.
    ///
    /// # Errors
    ///
    /// Returns the first failure to read a resource.
    pub fn load(&self, dictionary: &mut Dictionary) -> Result<Vec<Loaded>, Diagnostic> {
        let windows: Vec<Window> = (0..self.sources.len())
            .flat_map(|index| self.windows(index))
            .collect();
        // One load is one run of the sources' conditions.
        let context = EvaluationContext::now();
        // Several windows go to the pool whatever their size, so only a
        // lone window's file is measured.
        let tasks = if windows.len() > 1 || self.input_bytes() >= POOL_BYTES {
            Tasks::of(windows.len())
        } else {
            Tasks::sequential(windows.len())
        };

        let mut loads: Vec<Loaded> = self
            .sources
            .iter()
            .map(|source| Loaded::with_context(source.options.arity(), context))
            .collect();
        // Lines the earlier windows of each source consumed, and the
        // window being interned with the lines it consumed so far: a
        // window's line numbers continue from its source's base.
        let mut bases = vec![0u64; self.sources.len()];
        let mut current: Option<(usize, u64)> = None;
        let mut failure = None;
        let runs = RunPool::new(context);
        parallel::stream_tasks(
            tasks,
            |index| match windows[index].range {
                Some(_) => AHEAD_BYTES,
                None => WHOLE_AHEAD_BYTES,
            },
            |run: &Result<Parsed, Diagnostic>| run.as_ref().map_or(0, Parsed::bytes),
            |index, emit| self.parse(&windows[index], &runs, emit),
            |index, run| {
                let run = match run {
                    Ok(run) => run,
                    Err(diagnostic) => {
                        failure = Some(diagnostic);
                        return ControlFlow::Break(());
                    }
                };
                if let Some((window, consumed)) = current.filter(|&(window, _)| window != index) {
                    bases[windows[window].source] += consumed;
                }
                current = Some((index, run.consumed_lines));
                let source = windows[index].source;
                run.encode_into(&mut loads[source], dictionary, bases[source]);
                runs.give(run);
                ControlFlow::Continue(())
            },
        );
        match failure {
            Some(diagnostic) => Err(diagnostic),
            None => Ok(loads),
        }
    }
}

/// Returns the first byte after the record boundary at or past `target`,
/// found by scanning for a newline.
///
/// Returns `None` if no newline is in reach (a record longer than the scan window)
/// or the file cannot be read there.
fn next_record_boundary(file: &mut File, target: u64, size: u64) -> Option<u64> {
    /// How far past the target a record boundary is looked for.
    const SCAN: u64 = 1 << 20;
    /// The bytes one read of the scan takes.
    const CHUNK: usize = 1 << 16;

    file.seek(SeekFrom::Start(target)).ok()?;
    let reach = SCAN.min(size - target);
    let mut buffer = vec![0u8; CHUNK];
    let mut scanned = 0;
    while scanned < reach {
        let wanted = usize::try_from(reach - scanned).map_or(CHUNK, |left| left.min(CHUNK));
        let read = file.read(&mut buffer[..wanted]).ok()?;
        if read == 0 {
            return None;
        }
        if let Some(newline) = buffer[..read].iter().position(|&byte| byte == b'\n') {
            let boundary = target + scanned + newline as u64 + 1;
            return (boundary < size).then_some(boundary);
        }
        scanned += read as u64;
    }
    None
}

#[cfg(test)]
mod tests;
