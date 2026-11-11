//! Batches of delimiter-separated resources, parsed in parallel and
//! interned in resource order.
//!
//! A large resource is cut into *windows*, byte ranges that are parsed in parallel.

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
    dsv::{ColumnLayout, DsvReader, Parsed, ResourceLoad, RunPool},
    error::IoError,
    load::{DsvResource, Loaded, Resource},
};

/// The size a window aims for.
///
/// A resource of at least twice this size is cut into windows.
/// The common case is a batch with one bulk file,
/// which would stay on one thread if only whole resources were parsed in parallel.
/// The runs of a window need up to eight times the size of its text, for narrow rows of short
/// numbers, and [AHEAD_BYTES] has room for all of them.
const WINDOW_BYTES: u64 = 2 * 1024 * 1024;

/// The size of input from which a batch of a single window is parsed on the worker pool.
///
/// A batch of a single window with less input is parsed on the calling thread.
const POOL_BYTES: u64 = 64 * 1024;

/// The size of a window's runs that a worker may hold ready before they are interned.
///
/// A parse that is further ahead waits.
/// The windows are interned in order,
/// so a window is parsed in parallel only as far as its runs fit within this bound.
/// The bound has room for all runs of a [window](WINDOW_BYTES).
const AHEAD_BYTES: usize = 16 * 1024 * 1024;

/// The size of a whole resource's runs that a worker may hold ready before they are interned.
///
/// A resource that is not cut into windows has no bound on its size,
/// so this bound cannot have room for all its runs.
/// Room for a couple of runs keeps its parse running alongside its interning.
const WHOLE_AHEAD_BYTES: usize = 4 * 1024 * 1024;

/// One unit of parallel parsing: a whole resource, or a window of one.
#[derive(Debug)]
struct Window {
    /// The index of the resource that this window reads.
    source: usize,
    /// The index of this window among the resource's windows, in file order.
    position: usize,
    /// The byte range, or `None` for the whole resource.
    ///
    /// A whole resource is opened as a reader opens a file, with decompression.
    range: Option<std::ops::Range<u64>>,
}

/// A batch of delimiter-separated resources loaded together.
///
/// The resources are parsed in parallel, window by window.
/// A large resource that is unquoted, uncompressed and without a row limit is cut into windows
/// at record boundaries, so that one bulk resource keeps every worker busy.
/// Each window hands its rows on in runs of bounded size,
/// and the calling thread interns the runs into the dictionary as they arrive,
/// in the order of the resources and their windows.
/// So the dictionary has one writer,
/// and receives the values in the order that a sequential read would give them.
///
/// A worker's parse waits once the runs it holds ahead of the interning reach a bound,
/// so a batch keeps at most that much in memory per worker, however large its resources.
/// The bound has room for a whole window,
/// so the workers parse the windows after the one being interned in parallel.
/// A batch of one small window is parsed on the calling thread,
/// which interns each run as it is parsed.
#[derive(Debug)]
pub struct ResourceBatch {
    /// The resources, loaded in order.
    sources: Vec<DsvResource>,
    /// The size a window aims for, which is [WINDOW_BYTES] unless a test changes it.
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

    /// Returns the windows of the resource at `index`: byte ranges cut at record boundaries,
    /// or the whole resource if it cannot be cut.
    ///
    /// A resource is not cut
    /// - under quoting, since a newline is then not always a record boundary,
    /// - under compression, since a compressed stream cannot be read from its middle,
    /// - under a row limit, which counts rows in the resource's order across all windows,
    /// - if it is small, or
    /// - if a scan finds no record boundary.
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
                // No boundary within reach, so keep the windows found so far.
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

    /// Returns the size of the batch's input:
    /// the stored sizes of its files, compressed or not, and the sizes of its bytes in memory.
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

    /// Parses one window and passes its runs to `emit`.
    ///
    /// If the window cannot be read, the failure is passed to `emit` after the runs.
    /// `layout` says where the records have their cells, for a resource with named columns.
    fn parse(
        &self,
        window: &Window,
        runs: &RunPool,
        context: EvaluationContext,
        layout: Option<&ColumnLayout>,
        emit: &mut dyn FnMut(Result<Parsed, Diagnostic>) -> bool,
    ) {
        let source = &self.sources[window.source];
        let mut options = source.options.clone();

        // Only the first window of a resource contains the header to skip.
        options.ignore_headers =
            (source.options.ignore_headers || layout.is_some()) && window.position == 0;
        let reader = DsvReader::new(options);
        let mut deliver = |run: Parsed| emit(Ok(run));
        let result = match (&source.input, &window.range) {
            (Resource::Path(path), None) => source
                .options
                .compression
                .open_file(path)
                .and_then(|file| reader.parse(file, runs, context, layout, &mut deliver)),
            (Resource::Path(path), Some(range)) => File::open(path)
                .and_then(|mut file| file.seek(SeekFrom::Start(range.start)).map(|_| file))
                .map_err(|error| IoError::unreadable(path, error))
                .and_then(|file| {
                    let file = file.take(range.end - range.start);

                    reader.parse(file, runs, context, layout, &mut deliver)
                }),
            (Resource::Bytes { bytes, .. }, range) => {
                let slice: &[u8] = match range {
                    None => bytes,
                    // Bytes cut into windows are never compressed, since `windows`
                    // does not cut compressed ones, and the bounds came from this buffer.
                    Some(range) => {
                        let start = usize::try_from(range.start).expect("bytes are in memory");
                        let end = usize::try_from(range.end).expect("bytes are in memory");
                        &bytes[start..end]
                    }
                };
                if source.input.compressed(source.options.compression) {
                    let slice = MultiGzDecoder::new(slice);

                    reader.parse(slice, runs, context, layout, &mut deliver)
                } else {
                    reader.parse(slice, runs, context, layout, &mut deliver)
                }
            }
        };
        if let Err(diagnostic) = result {
            emit(Err(diagnostic.with_resource(source.input.name())));
        }
    }

    /// Returns where the records of the resource at `index` have the cells of its named
    /// columns, read from its header, or `None` for a resource whose columns go by position.
    fn header_layout(&self, index: usize) -> Result<Option<ColumnLayout>, Diagnostic> {
        let source = &self.sources[index];
        if source.options.columns.is_none() {
            return Ok(None);
        }

        let reader = DsvReader::new(source.options.clone());
        let compression = source.options.compression;
        let layout = match &source.input {
            Resource::Path(path) => compression
                .open_file(path)
                .and_then(|file| reader.header_layout(file)),
            Resource::Bytes { name, bytes } => {
                reader.header_layout(compression.open_bytes(bytes.clone(), name))
            }
        };

        layout.map_err(|diagnostic| diagnostic.with_resource(source.input.name()))
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
        // The header of a resource is read before its windows are parsed,
        // since only the first window contains it.
        let layouts: Vec<Option<ColumnLayout>> = (0..self.sources.len())
            .map(|index| self.header_layout(index))
            .collect::<Result<_, _>>()?;

        let windows: Vec<Window> = (0..self.sources.len())
            .flat_map(|index| self.windows(index))
            .collect();

        // Several windows go to the worker pool whatever their size, so only
        // the input of a single window is measured.
        let tasks = if windows.len() > 1 || self.input_bytes() >= POOL_BYTES {
            Tasks::of(windows.len())
        } else {
            Tasks::sequential(windows.len())
        };

        let mut loads: Vec<ResourceLoad<'_>> = self
            .sources
            .iter()
            .map(|source| ResourceLoad::new(&source.options, context))
            .collect();

        // For each resource, the lines that its earlier windows consumed,
        // and the window being interned with the lines it consumed so far.
        // A window's line numbers continue from its resource's base.
        let mut bases = vec![0u64; self.sources.len()];
        let mut current: Option<(usize, u64)> = None;
        let mut failure = None;
        let runs = RunPool::default();
        parallel::stream_tasks(
            tasks,
            |index| match windows[index].range {
                Some(_) => AHEAD_BYTES,
                None => WHOLE_AHEAD_BYTES,
            },
            |run: &Result<Parsed, Diagnostic>| run.as_ref().map_or(0, Parsed::bytes),
            |index, emit| {
                let layout = layouts[windows[index].source].as_ref();

                self.parse(&windows[index], &runs, context, layout, emit)
            },
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
        let loads = loads
            .into_iter()
            .map(|load| load.finish(dictionary))
            .collect();

        match failure {
            Some(diagnostic) => Err(diagnostic),
            None => Ok(loads),
        }
    }
}

/// Returns the position of the first byte after the first newline at or after `target`.
///
/// `size` is the size of the file.
/// Returns `None` if no newline is within reach, as for a record longer than the scan,
/// if the newline ends the file, or if the file cannot be read there.
fn next_record_boundary(file: &mut File, target: u64, size: u64) -> Option<u64> {
    /// How far past the target a record boundary is looked for.
    const SCAN: u64 = 1 << 20;
    /// The size of one read of the scan.
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
