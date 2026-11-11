//! Batches of resources that are parsed in parallel and interned in resource order.
//!
//! A large resource is cut into *windows*, byte ranges that end at line ends and are parsed in
//! parallel.
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
    parallel::{self, Tasks},
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
/// The runs of a window need up to eight times the size of its text, for narrow rows of short
/// numbers, and [AHEAD_BYTES] has room for all of them.
pub(crate) const WINDOW_BYTES: u64 = 2 * 1024 * 1024;

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

    /// Returns whether the resource's format allows cutting it into windows at line ends.
    ///
    /// A line end is then a boundary between rows,
    /// and no row depends on the rows before it.
    fn cuts_at_lines(&self) -> bool;

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

/// Returns the windows of the resource at `index`: byte ranges cut at line ends,
/// or the whole resource if it cannot be cut.
///
/// A resource is not cut
/// - if its format does not [allow](RunSource::cuts_at_lines) it,
/// - under compression, since a compressed stream cannot be read from its middle,
/// - if it is small, or
/// - if a scan finds no line end.
pub(crate) fn windows<S: RunSource>(source: &S, index: usize, window_bytes: u64) -> Vec<Window> {
    let whole = vec![Window {
        source: index,
        position: 0,
        range: None,
    }];
    let input = source.input();
    if !source.cuts_at_lines() || input.compressed(source.compression()) {
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

    let mut file = match input {
        Resource::Path(path) => match File::open(path) {
            Ok(file) => Some(file),
            Err(_) => return whole,
        },
        Resource::Bytes { .. } => None,
    };

    let count = usize::try_from(size / window_bytes)
        .unwrap_or(usize::MAX)
        .max(2);
    let mut bounds = vec![0u64];
    for split in 1..count {
        let target = size * split as u64 / count as u64;
        let boundary = match (&mut file, input) {
            (Some(file), _) => next_line_start(file, target, size),
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
            // No line end within reach, so keep the windows found so far.
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
    let windows: Vec<Window> = sources
        .iter()
        .enumerate()
        .flat_map(|(index, source)| windows(source, index, window_bytes))
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
