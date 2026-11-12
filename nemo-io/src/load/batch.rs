//! Batches of resources that are parsed in parallel and interned in resource order.
//!
//! A large resource is cut into *windows*, byte ranges that end where a row ends and are parsed
//! in parallel.
//! Each window hands its rows on in runs of bounded size,
//! and the calling thread interns the runs into the dictionary as they arrive,
//! in the order of the resources and their windows.
//! So the dictionary has one writer,
//! and receives the values in the order that a sequential read would give them.

use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    ops::{ControlFlow, Range},
};

use nemo_physical::{
    diagnostics::Diagnostic,
    dictionary::Dictionary,
    function::EvaluationContext,
    parallel::{self, Tasks, Workers},
};

use crate::{
    AboutResource,
    compression::Compression,
    error::IoError,
    load::{Loaded, Resource, run::RunPool},
};

/// The size a window aims for.
///
/// A resource of at least twice this size is cut into windows.
/// The common case is a batch with one bulk file,
/// which would stay on one thread if only whole resources were parsed in parallel.
/// The runs of a window need about three times the size of its text for narrow rows of
/// numbers, and less for wider rows, and [AHEAD_BYTES] has room for them.
pub(crate) const WINDOW_BYTES: u64 = 1024 * 1024;

/// The size of input from which a batch of a single window is parsed on the worker pool.
///
/// A batch of a single window with less input is parsed on the calling thread.
const POOL_BYTES: u64 = 64 * 1024;

/// The size of a window's runs that a worker may hold ready before they are interned.
///
/// A parse that is further ahead waits.
/// The windows are interned in order,
/// so a window is parsed in parallel only as far as its runs fit within this bound.
/// The bound has room for all runs of a [window](WINDOW_BYTES), and for little more:
/// every worker may hold this much in memory beside the stored rows,
/// and loads were measured to be no faster with a larger bound.
const AHEAD_BYTES: usize = 4 * 1024 * 1024;

/// The size of a whole resource's runs that a worker may hold ready before they are interned.
///
/// A resource that is not cut into windows has no bound on its size,
/// so this bound cannot have room for all its runs.
/// Room for a couple of runs keeps its parse running alongside its interning.
const WHOLE_AHEAD_BYTES: usize = 4 * 1024 * 1024;

/// The size of one read of a scan for the row ends of a resource.
const SCAN_CHUNK: usize = 1 << 18;

/// Where a resource may be cut into windows.
pub(crate) enum Cuts {
    /// Nowhere, so the resource is one window.
    Nowhere,
    /// After any line end.
    ///
    /// A line end is then a boundary between rows, and no row depends on the rows before it.
    Lines,
    /// After the line ends that end a row, where other line ends are part of a row.
    ///
    /// Which line ends these are depends on the bytes before them,
    /// so the resource is read once from its start to find them.
    RowEnds(Box<dyn RowEnds>),
}

/// Finds the line ends of a resource that end a row.
///
/// The bytes of the resource are passed in order, from its start.
pub(crate) trait RowEnds {
    /// Reads the next bytes of the resource, up to and including the first line end that ends
    /// a row and is at index `from` or after it, and returns how many bytes that is.
    ///
    /// Returns `None` if the bytes have no such line end, and reads all of them then.
    fn advance(&mut self, bytes: &[u8], from: usize) -> Option<usize>;
}

/// One unit of parallel parsing: a whole resource, or a window of one.
#[derive(Debug)]
pub(crate) struct Window {
    /// The index of the resource that this window reads.
    source: usize,
    /// The index of this window among the resource's windows, in file order.
    position: usize,
    /// The byte range, or `None` for the whole resource.
    ///
    /// A whole resource is opened as a reader opens a file, with decompression.
    range: Option<Range<u64>>,
}

impl Window {
    /// Returns the window that is the whole resource at index `source`.
    fn whole(source: usize) -> Self {
        Self {
            source,
            position: 0,
            range: None,
        }
    }
}

/// A resource of a format whose rows are parsed into runs away from the dictionary.
///
/// The batch opens the resource and cuts it into windows,
/// and the format parses each window into runs and interns the runs in order.
pub(crate) trait RunSource: Sync {
    /// A run of parsed rows, which awaits interning.
    type Run: Send;
    /// What is read from the resource before its windows are parsed, such as a header.
    type Preface: Sync;
    /// The load of the resource while its runs are interned.
    type Load<'a>
    where
        Self: 'a;

    /// Returns where the resource's bytes come from.
    fn input(&self) -> &Resource;

    /// Returns the compression of the resource's stream.
    fn compression(&self) -> Compression;

    /// Returns where the resource may be cut into windows.
    fn cuts(&self) -> Cuts;

    /// Reads what the parse of every window needs from the start of the resource.
    fn preface(&self) -> Result<Self::Preface, Diagnostic>;

    /// Parses one window into runs, and passes each to `emit` once it is full,
    /// until `emit` returns `false`.
    ///
    /// `first` says whether the window starts the resource.
    /// The last run that is passed has the number of lines that the parse consumed.
    fn parse(
        &self,
        source: impl Read,
        first: bool,
        preface: &Self::Preface,
        runs: &RunPool<Self::Run>,
        context: EvaluationContext,
        emit: &mut dyn FnMut(Self::Run) -> bool,
    ) -> Result<(), Diagnostic>;

    /// Returns the size in bytes of a run's buffers.
    fn run_bytes(run: &Self::Run) -> usize;

    /// Returns the number of lines that the parse of a window consumed,
    /// which its last run has, and `0` for any other run.
    fn consumed_lines(run: &Self::Run) -> u64;

    /// Starts the load of the resource.
    fn start(&self, dictionary: &mut Dictionary, context: EvaluationContext) -> Self::Load<'_>;

    /// Interns the rows of a run into the load of its resource.
    ///
    /// `base` is the number of lines that earlier windows of the resource consumed,
    /// which is added to the run's line numbers.
    fn intern(run: &Self::Run, load: &mut Self::Load<'_>, dictionary: &mut Dictionary, base: u64);

    /// Ends the load of the resource.
    fn finish(load: Self::Load<'_>, dictionary: &mut Dictionary) -> Loaded;
}

/// Opens a whole resource as a stream, with decompression.
pub(crate) fn open(
    input: &Resource,
    compression: Compression,
) -> Result<Box<dyn Read>, Diagnostic> {
    match input {
        Resource::Path(path) => compression.open_file(path),
        Resource::Bytes { name, bytes } => Ok(compression.open_bytes(bytes.clone(), name)),
    }
}

/// Returns the windows of the resource at `index`: byte ranges cut where a row ends,
/// or the whole resource if it cannot be cut.
///
/// A resource is not cut
/// - if it may be cut [nowhere](RunSource::cuts),
/// - under compression, since a compressed stream cannot be read from its middle,
/// - if it is small, or
/// - if no row is found to end where a window should.
pub(crate) fn windows<S: RunSource>(source: &S, index: usize, window_bytes: u64) -> Vec<Window> {
    let whole = vec![Window::whole(index)];
    let input = source.input();
    let cuts = source.cuts();
    if matches!(cuts, Cuts::Nowhere) || input.compressed(source.compression()) {
        return whole;
    }

    let size = match input {
        Resource::Path(path) => match std::fs::metadata(path) {
            Ok(data) => data.len(),
            // The parse will report the unreadable file itself.
            Err(_) => return whole,
        },
        Resource::Bytes { bytes, .. } => bytes.len() as u64,
    };
    if size < window_bytes * 2 {
        return whole;
    }

    let mut starts: Box<dyn FnMut(u64) -> Option<u64> + '_> = match cuts {
        Cuts::Nowhere => return whole,
        Cuts::Lines => {
            let mut file = match input {
                Resource::Path(path) => match File::open(path) {
                    Ok(file) => Some(file),
                    Err(_) => return whole,
                },
                Resource::Bytes { .. } => None,
            };

            Box::new(move |target| match (&mut file, input) {
                (Some(file), _) => next_line_start(file, target, size),
                (None, Resource::Bytes { bytes, .. }) => bytes
                    [usize::try_from(target).expect("bytes are in memory")..]
                    .iter()
                    .position(|&byte| byte == b'\n')
                    .map(|newline| target + newline as u64 + 1)
                    .filter(|&boundary| boundary < size),
                (None, Resource::Path(_)) => unreachable!("paths opened their file above"),
            })
        }
        Cuts::RowEnds(row_ends) => {
            let stream: Box<dyn Read + '_> = match input {
                Resource::Path(path) => match File::open(path) {
                    Ok(file) => Box::new(file),
                    Err(_) => return whole,
                },
                Resource::Bytes { bytes, .. } => Box::new(&bytes[..]),
            };
            let mut scan = RowScan::new(stream, row_ends);

            Box::new(move |target| scan.row_start(target).filter(|&boundary| boundary < size))
        }
    };

    let count = usize::try_from(size / window_bytes)
        .unwrap_or(usize::MAX)
        .max(2);
    let mut bounds = vec![0u64];
    for split in 1..count {
        let target = size * split as u64 / count as u64;
        match starts(target) {
            Some(boundary) if boundary > *bounds.last().expect("bounds start filled") => {
                bounds.push(boundary)
            }
            Some(_) => {}
            // No row end within reach, so keep the windows found so far.
            None => break,
        }
    }

    drop(starts);

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

/// A scan of a resource from its start for the positions at which rows start.
struct RowScan<'a> {
    /// The bytes of the resource.
    stream: Box<dyn Read + 'a>,
    /// What finds the line ends that end a row.
    row_ends: Box<dyn RowEnds>,
    /// The bytes read from the stream, of which those from `start` on are not scanned yet.
    buffer: Vec<u8>,
    /// The index in the buffer of the first byte that is not scanned yet.
    start: usize,
    /// The position in the resource of that byte.
    position: u64,
}

impl<'a> RowScan<'a> {
    /// Creates a scan of the given bytes, which start the resource.
    fn new(stream: Box<dyn Read + 'a>, row_ends: Box<dyn RowEnds>) -> Self {
        Self {
            stream,
            row_ends,
            buffer: Vec::new(),
            start: 0,
            position: 0,
        }
    }

    /// Returns the position of the first byte after the first line end that ends a row and is
    /// at `target` or after it.
    ///
    /// The targets of successive calls must not decrease.
    /// Returns `None` if there is no such line end, or if the resource cannot be read.
    fn row_start(&mut self, target: u64) -> Option<u64> {
        loop {
            if self.start == self.buffer.len() {
                self.buffer.resize(SCAN_CHUNK, 0);
                let read = self.stream.read(&mut self.buffer).ok()?;
                self.buffer.truncate(read);
                self.start = 0;
                if read == 0 {
                    return None;
                }
            }

            let bytes = &self.buffer[self.start..];
            let from = usize::try_from(target.saturating_sub(self.position))
                .map_or(bytes.len(), |from| from.min(bytes.len()));
            let scanned = self.row_ends.advance(bytes, from);
            let read = scanned.unwrap_or(bytes.len());
            self.start += read;
            self.position += read as u64;
            if scanned.is_some() {
                return Some(self.position);
            }
        }
    }
}

/// Returns the size of the input of resources:
/// the stored sizes of their files, compressed or not, and the sizes of their bytes in memory.
fn input_bytes<S: RunSource>(sources: &[S]) -> u64 {
    sources
        .iter()
        .map(|source| match source.input() {
            // The parse will report an unreadable file itself.
            Resource::Path(path) => std::fs::metadata(path).map_or(0, |data| data.len()),
            Resource::Bytes { bytes, .. } => bytes.len() as u64,
        })
        .sum()
}

/// Parses one window and passes its runs to `emit`.
///
/// If the window cannot be read, the failure is passed to `emit` after the runs.
fn parse<S: RunSource>(
    source: &S,
    window: &Window,
    preface: &S::Preface,
    runs: &RunPool<S::Run>,
    context: EvaluationContext,
    emit: &mut dyn FnMut(Result<S::Run, Diagnostic>) -> bool,
) {
    let input = source.input();
    let first = window.position == 0;
    let mut deliver = |run: S::Run| emit(Ok(run));
    let result = match (input, &window.range) {
        (_, None) => open(input, source.compression())
            .and_then(|stream| source.parse(stream, first, preface, runs, context, &mut deliver)),
        (Resource::Path(path), Some(range)) => File::open(path)
            .and_then(|mut file| file.seek(SeekFrom::Start(range.start)).map(|_| file))
            .map_err(|error| IoError::unreadable(path, error))
            .and_then(|file| {
                let file = file.take(range.end - range.start);

                source.parse(file, first, preface, runs, context, &mut deliver)
            }),

        // Bytes cut into windows are never compressed, and the bounds came from this buffer.
        (Resource::Bytes { bytes, .. }, Some(range)) => {
            let start = usize::try_from(range.start).expect("bytes are in memory");
            let end = usize::try_from(range.end).expect("bytes are in memory");

            source.parse(
                &bytes[start..end],
                first,
                preface,
                runs,
                context,
                &mut deliver,
            )
        }
    };
    if let Err(diagnostic) = result {
        emit(Err(diagnostic.with_resource(input.name())));
    }
}

/// Loads every resource, and returns the loads in the order of the resources.
///
/// The resources are parsed in parallel, window by window.
/// A worker's parse waits once the runs it holds ahead of the interning reach a bound,
/// so a batch keeps at most that much in memory per worker, however large its resources.
/// A batch of one small window is parsed on the calling thread,
/// which interns each run as it is parsed.
///
/// `window_bytes` is the size that a window aims for,
/// and `context` is the evaluation context of the resources' conditions.
///
/// # Errors
///
/// Returns the first failure to read a resource.
pub(crate) fn load<S: RunSource>(
    sources: &[S],
    window_bytes: u64,
    dictionary: &mut Dictionary,
    context: EvaluationContext,
) -> Result<Vec<Loaded>, Diagnostic> {
    // What every window of a resource needs is read before the windows are parsed,
    // since only the first window contains it.
    let prefaces: Vec<S::Preface> = sources
        .iter()
        .map(|source| {
            source
                .preface()
                .map_err(|diagnostic| diagnostic.with_resource(source.input().name()))
        })
        .collect::<Result<_, _>>()?;

    // A single thread parses the windows one after another, as it parses a whole resource,
    // so a scan of the resource for its windows would gain nothing.
    let single = Workers::get().threads() <= 1;
    let windows: Vec<Window> = sources
        .iter()
        .enumerate()
        .flat_map(|(index, source)| {
            if single && matches!(source.cuts(), Cuts::RowEnds(_)) {
                vec![Window::whole(index)]
            } else {
                windows(source, index, window_bytes)
            }
        })
        .collect();

    // Several windows go to the worker pool whatever their size, so only
    // the input of a single window is measured.
    let tasks = if windows.len() > 1 || input_bytes(sources) >= POOL_BYTES {
        Tasks::of(windows.len())
    } else {
        Tasks::sequential(windows.len())
    };

    let mut loads: Vec<S::Load<'_>> = sources
        .iter()
        .map(|source| source.start(dictionary, context))
        .collect();

    // For each resource, the lines that its earlier windows consumed,
    // and the window being interned with the lines it consumed so far.
    // A window's line numbers continue from its resource's base.
    let mut bases = vec![0u64; sources.len()];
    let mut current: Option<(usize, u64)> = None;
    let mut failure = None;
    let runs = RunPool::default();
    parallel::stream_tasks(
        tasks,
        |index| match windows[index].range {
            Some(_) => AHEAD_BYTES,
            None => WHOLE_AHEAD_BYTES,
        },
        |run: &Result<S::Run, Diagnostic>| run.as_ref().map_or(0, S::run_bytes),
        |index, emit| {
            let window = &windows[index];
            let source = &sources[window.source];

            parse(
                source,
                window,
                &prefaces[window.source],
                &runs,
                context,
                emit,
            );
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

            current = Some((index, S::consumed_lines(&run)));
            let source = windows[index].source;
            S::intern(&run, &mut loads[source], dictionary, bases[source]);
            runs.give(run);

            ControlFlow::Continue(())
        },
    );
    let loads = loads
        .into_iter()
        .map(|load| S::finish(load, dictionary))
        .collect();

    match failure {
        Some(diagnostic) => Err(diagnostic),
        None => Ok(loads),
    }
}

/// Returns the position of the first byte after the first newline at or after `target`.
///
/// `size` is the size of the file.
/// Returns `None` if no newline is within reach, as for a line longer than the scan,
/// if the newline ends the file, or if the file cannot be read there.
fn next_line_start(file: &mut File, target: u64, size: u64) -> Option<u64> {
    /// How far past the target a line end is looked for.
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
