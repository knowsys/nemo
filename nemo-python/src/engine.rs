//! The `Engine` class, which loads programs, reasons, and reads the results.

use std::{
    collections::VecDeque,
    sync::{
        Mutex, MutexGuard, TryLockError,
        mpsc::{self, RecvTimeoutError},
    },
    time::Duration,
};

use pyo3::{
    prelude::*,
    types::{PyList, PyTuple},
};

use nemo::{
    diagnostics::render::SourceTexts,
    engine::PredicateCursor,
    trace::TraceResponse,
    values::{DataValue, TypeSyntax},
};

use crate::{
    errors::{Diagnostic, NemoError, raise, resolve},
    program::Program,
    values::{to_python, to_value},
};

/// How often a running call checks for Ctrl-C.
const SIGNAL_POLL: Duration = Duration::from_millis(50);

/// The state behind the engine's lock: the engine,
/// and the sources against which its diagnostics are rendered.
///
/// The two change together, so one lock protects both.
#[derive(Debug, Default)]
pub(crate) struct Inner {
    /// The wrapped engine.
    pub engine: nemo::engine::Engine,
    /// The texts of the engine's sources, for rendering diagnostics.
    pub sources: SourceTexts,
}

impl Inner {
    /// Returns the `DiagnosticsError` of one diagnostic, rendered against the engine's sources.
    fn raise(&self, py: Python<'_>, diagnostic: nemo::diagnostics::Diagnostic) -> PyErr {
        raise(py, vec![diagnostic], &self.sources)
    }
}

/// The reasoning engine for one program,
/// which loads the program, reasons to the fixpoint, and reads the results.
///
/// One method runs at a time, except `cancel`.
/// A method called from another Python thread while one runs raises `NemoError` instead of
/// waiting, since waiting while holding the GIL would deadlock with a run that polls for
/// Ctrl-C.
#[pyclass(frozen, module = "nmo")]
#[derive(Debug)]
pub struct Engine {
    /// The engine and its sources, behind the lock that lets one method run at a time.
    inner: Mutex<Inner>,
    /// The engine's cancel token, kept outside the lock,
    /// so that another thread can cancel a running method.
    token: nemo::engine::CancelToken,
}

impl Default for Engine {
    fn default() -> Self {
        let inner = Inner::default();
        let token = inner.engine.cancel_token();
        Self {
            inner: Mutex::new(inner),
            token,
        }
    }
}

impl Engine {
    /// Passes `program` to `call`, a loading method of the engine,
    /// and raises the diagnostics of a failure.
    ///
    /// [Engine::ingest](nemo::engine::Engine::ingest) parses the program's text again,
    /// under the engine's own file id.
    /// The call runs as [interruptible](Self::interruptible) describes.
    fn ingest(
        &self,
        py: Python<'_>,
        program: &Program,
        call: impl FnOnce(
            &mut nemo::engine::Engine,
            nemo::model::Program,
        ) -> Result<(), nemo::diagnostics::Diagnostic>
        + Send,
    ) -> PyResult<()> {
        let mut inner = self.lock()?;
        let inner = &mut *inner;
        let name = program.name.as_str();
        let text = program.text.clone();
        let engine = &mut inner.engine;
        let outcome = self.interruptible(py, || {
            engine.ingest(name, text, |engine, model| call(engine, model))
        })?;
        // Linking reads the program's modules, so their sources are added before the
        // outcome is rendered against them.
        inner.sources.absorb(inner.engine.sources());
        outcome.map_err(|diagnostics| raise(py, diagnostics, &inner.sources))
    }

    /// Runs `call` with the GIL released, on a thread of its own,
    /// while this thread polls for signals every [SIGNAL_POLL].
    ///
    /// It returns as soon as the call ends.
    /// Ctrl-C cancels the call through the engine's cancel token.
    /// Once the call has stopped, `KeyboardInterrupt` is raised in place of its outcome.
    fn interruptible<T: Send>(
        &self,
        py: Python<'_>,
        call: impl FnOnce() -> T + Send,
    ) -> PyResult<T> {
        let token = self.token.clone();
        let mut interrupt: Option<PyErr> = None;
        let outcome = py.detach(|| {
            std::thread::scope(|scope| {
                // The wait ends when the worker sends on the channel as it ends,
                // or when a worker that panics drops the sender.
                let (sender, receiver) = mpsc::channel();
                let worker = scope.spawn(move || {
                    let outcome = call();
                    sender.send(()).ok();
                    outcome
                });
                while let Err(RecvTimeoutError::Timeout) = receiver.recv_timeout(SIGNAL_POLL) {
                    if interrupt.is_none()
                        && let Err(signal) = Python::attach(|py| py.check_signals())
                    {
                        token.cancel();
                        interrupt = Some(signal);
                    }
                }
                match worker.join() {
                    Ok(outcome) => outcome,
                    Err(panic) => std::panic::resume_unwind(panic),
                }
            })
        });
        match interrupt {
            Some(signal) => Err(signal),
            None => Ok(outcome),
        }
    }

    /// Locks the engine's state.
    ///
    /// Raises `NemoError` if a method is running in another thread,
    /// or if an earlier method panicked.
    fn lock(&self) -> PyResult<MutexGuard<'_, Inner>> {
        match self.inner.try_lock() {
            Ok(guard) => Ok(guard),
            Err(TryLockError::WouldBlock) => Err(NemoError::new_err(
                "the engine is busy in another thread; wait for that call to finish",
            )),
            Err(TryLockError::Poisoned(_)) => Err(NemoError::new_err(
                "the engine is unusable because an earlier call panicked",
            )),
        }
    }
}

#[pymethods]
impl Engine {
    /// Creates an empty engine.
    #[new]
    fn new() -> Self {
        Self::default()
    }

    /// Adds rows from memory to a predicate.
    ///
    /// `rows` is an iterable of rows, each an iterable of values,
    /// which are converted by the module's conversion table.
    /// Before `load`, the rows are kept, and `load` checks them as it checks the program's facts
    /// and stores them with these.
    /// After it, they are new facts, and the next `reason()` continues to the new fixpoint.
    /// The predicate must then be one that the program uses or that rows were given to,
    /// since only `extend` adds new ones.
    /// Raises `ValueError` or `TypeError` for a value that does not convert,
    /// and `DiagnosticsError` for a row that the predicate does not accept,
    /// or with code N0493 while a run is suspended (see `discard_pending`).
    fn import_rows(
        &self,
        py: Python<'_>,
        predicate: &str,
        rows: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let mut converted: Vec<Vec<DataValue>> = Vec::new();
        for row in rows.try_iter()? {
            let row = row?;
            let mut values = Vec::new();
            for value in row.try_iter()? {
                values.push(to_value(&value?)?);
            }
            converted.push(values);
        }
        let mut inner = self.lock()?;
        inner
            .engine
            .import_rows(predicate, converted)
            .map_err(|diagnostic| inner.raise(py, diagnostic))
    }

    /// Registers a resource in memory under `name`.
    ///
    /// An `@import` of a resource with that name reads `content`, a `str` or `bytes`,
    /// instead of the file system.
    /// Raises `TypeError` if `content` is neither.
    fn add_resource(&self, name: &str, content: &Bound<'_, PyAny>) -> PyResult<()> {
        let bytes: Vec<u8> = match content.extract::<String>() {
            Ok(text) => text.into_bytes(),
            Err(_) => content.extract::<Vec<u8>>()?,
        };
        self.lock()?.engine.add_resource(name, bytes);
        Ok(())
    }

    /// Registers the text of a module under `name`, the name with which a `@use` refers to it,
    /// such as `lib::tc`.
    ///
    /// A `@use` of the module then reads this text instead of its file.
    /// Raises `DiagnosticsError` if `name` is not a module name.
    fn add_module(&self, py: Python<'_>, name: &str, text: &str) -> PyResult<()> {
        let mut inner = self.lock()?;
        inner
            .engine
            .add_module(name, text)
            .map_err(|diagnostic| inner.raise(py, diagnostic))
    }

    /// Declares a module root, a directory whose modules a `@use` refers to as `name::module`.
    ///
    /// Raises `DiagnosticsError` if `name` is not a single plain name, or is `super`.
    fn add_module_root(&self, py: Python<'_>, name: &str, directory: &str) -> PyResult<()> {
        let mut inner = self.lock()?;
        inner
            .engine
            .add_module_root(name, directory)
            .map_err(|diagnostic| inner.raise(py, diagnostic))
    }

    /// Sets the directory against which relative import resources are resolved,
    /// instead of the directory of the program file that contains the import.
    fn set_import_directory(&self, directory: &str) -> PyResult<()> {
        self.lock()?.engine.set_import_directory(directory);
        Ok(())
    }

    /// Keeps every imported predicate whole.
    ///
    /// Without it, loading stores only the columns and rows of an import that the rules read,
    /// and tracing or reading the rest raises `DiagnosticsError`.
    /// Call it before `load`.
    fn retain_imports(&self) -> PyResult<()> {
        self.lock()?.engine.retain_imports();
        Ok(())
    }

    /// Loads a program: it stores the facts, reads the imports and compiles the rules.
    ///
    /// Relative resources are resolved against the directory of the program's file
    /// (see `set_import_directory`), and remote ones are fetched, with retries.
    /// Ctrl-C cancels the load, which then changes nothing, and raises `KeyboardInterrupt`.
    /// Raises `DiagnosticsError` if the program does not parse or load,
    /// and with code N0493 while a run is suspended (see `discard_pending`).
    fn load(&self, py: Python<'_>, program: &Program) -> PyResult<()> {
        self.ingest(py, program, |engine, model| engine.load(model))
    }

    /// Extends the loaded program with the statements of `program`.
    ///
    /// The new facts and imports are stored, the whole program is compiled again,
    /// and the next `reason()` continues to the new fixpoint.
    /// An extension cannot change what the loaded statements mean,
    /// so a new `@base`, a prefix bound to another IRI, or a parameter declared again
    /// raises `DiagnosticsError`, since these need a `reload`.
    /// Ctrl-C cancels it as it cancels `load`.
    /// It also raises `DiagnosticsError` with code N0493 while a run is suspended
    /// (see `discard_pending`).
    fn extend(&self, py: Python<'_>, program: &Program) -> PyResult<()> {
        self.ingest(py, program, |engine, model| engine.extend(model))
    }

    /// Replaces the loaded program with `program`.
    ///
    /// Every derived row is dropped, every rule starts over,
    /// and the next `reason()` derives everything again from the input facts.
    /// A predicate whose facts, imports and declarations are unchanged keeps its input facts,
    /// and an unchanged import is not read again.
    /// The rows added through `import_rows` to a predicate whose definition changed are
    /// dropped, with a warning.
    /// Ctrl-C cancels it as it cancels `load`.
    /// Raises `DiagnosticsError` if the program does not parse or load,
    /// and with code N0493 while a run is suspended (see `discard_pending`).
    fn reload(&self, py: Python<'_>, program: &Program) -> PyResult<()> {
        self.ingest(py, program, |engine, model| engine.reload(model))
    }

    /// Checks a program without loading it, and returns its diagnostics.
    ///
    /// No data is read and nothing is stored.
    /// Every problem, warnings included, is returned rather than raised.
    fn check(&self, program: &Program) -> PyResult<Vec<Diagnostic>> {
        let mut inner = self.lock()?;
        let inner = &mut *inner;
        let diagnostics = inner
            .engine
            .check_source(program.name.as_str(), program.text.clone())
            .into_diagnostics();
        inner.sources.absorb(inner.engine.sources());
        Ok(diagnostics
            .iter()
            .map(|diagnostic| resolve(diagnostic, &inner.sources))
            .collect())
    }

    /// Reasons to the fixpoint by semi-naive evaluation,
    /// and fetches the data that bound imports need from the network.
    ///
    /// The GIL is released for the whole run.
    /// Ctrl-C cancels the run between rule applications, or while it waits for data from the
    /// network, and raises `KeyboardInterrupt`.
    /// Calling `reason()` again continues the run.
    /// Raises `DiagnosticsError` if no program is loaded, or if a rule application fails.
    fn reason(&self, py: Python<'_>) -> PyResult<()> {
        let mut inner = self.lock()?;
        let engine = &mut inner.engine;
        let outcome = self.interruptible(py, || engine.reason())?;
        outcome.map_err(|diagnostic| inner.raise(py, diagnostic))
    }

    /// Cancels the method that is running in another thread.
    ///
    /// A run stops between rule applications,
    /// and a method that waits for data from the network stops waiting.
    /// The cancelled method then raises `DiagnosticsError`.
    /// In the thread that runs the method, Ctrl-C cancels it instead.
    /// A cancellation before a method starts has no effect,
    /// since each method clears it when it starts.
    fn cancel(&self) {
        self.token.cancel();
    }

    /// Discards a run that is suspended for data from the network,
    /// and returns whether there was one.
    ///
    /// A run that is cancelled while it waits for data from the network stays suspended,
    /// and `reason()` continues it.
    /// While it is suspended, `load`, `extend`, `reload` and `import_rows` raise
    /// `DiagnosticsError` with code N0493.
    /// Discarding the run stops it as a cancellation between rule applications does,
    /// and the next `reason()` continues toward the fixpoint.
    fn discard_pending(&self) -> PyResult<bool> {
        Ok(self.lock()?.engine.discard_pending())
    }

    /// Traces facts, and returns the trace response as a dict.
    ///
    /// `facts` is one fact as a string, such as `"reach(a, b)"`, or a list of such strings,
    /// and the terminating `.` may be left out.
    /// The response lists, for each fact, its node if the fact holds,
    /// and the nodes of the derivations.
    /// A node's `rule` is the dict of its rule, and its `premises` are the nodes of its
    /// premises, so a derivation can be walked like a tree,
    /// and a fact that several derivations use is one dict.
    /// Raises `DiagnosticsError` for a text that is not a fact, or if the facts cannot be
    /// traced, for example while the engine holds updates that `reason()` has not processed.
    fn trace(&self, py: Python<'_>, facts: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let mut inner = self.lock()?;
        let facts = trace_facts(py, facts, &inner)?;
        let response = inner
            .engine
            .trace(&facts)
            .map_err(|diagnostic| inner.raise(py, diagnostic))?;
        let value = into_python(py, &response)?;
        linked(py, &response, value.bind(py))?;
        Ok(value)
    }

    /// Traces facts as `trace` does, and returns the response as ASCII proof trees.
    fn trace_ascii(&self, py: Python<'_>, facts: &Bound<'_, PyAny>) -> PyResult<String> {
        let mut inner = self.lock()?;
        let facts = trace_facts(py, facts, &inner)?;
        let response = inner
            .engine
            .trace(&facts)
            .map_err(|diagnostic| inner.raise(py, diagnostic))?;
        Ok(response.ascii())
    }

    /// Answers a `treeForTable` query of the explanation view, given as a dict.
    ///
    /// The response, also a dict, contains the facts that the query selects,
    /// with their derivations expanded.
    /// The query and the response use the protocol's camelCase keys.
    /// Raises `NemoError` for a query that does not have the protocol's shape,
    /// and `DiagnosticsError` if the engine cannot answer it.
    fn tree_for_table(&self, py: Python<'_>, query: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let query = from_python(py, query)?;
        let mut inner = self.lock()?;
        let response = inner
            .engine
            .tree_for_table(query)
            .map_err(|diagnostic| inner.raise(py, diagnostic))?;
        into_python(py, &response)
    }

    /// Answers a `tableEntriesForTreeNodes` query of the explanation view, given as a dict.
    ///
    /// The response is a list of dicts, one for each node of the tree that the query
    /// describes, with the node's facts, parents before children.
    /// The query and the response use the protocol's camelCase keys.
    /// Raises as `tree_for_table` does.
    fn table_entries_for_tree_nodes(
        &self,
        py: Python<'_>,
        query: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let query = from_python(py, query)?;
        let mut inner = self.lock()?;
        let response = inner
            .engine
            .table_entries_for_tree_nodes(query)
            .map_err(|diagnostic| inner.raise(py, diagnostic))?;
        into_python(py, &response)
    }

    /// Returns the rule with the statement id `id`, by which traces and queries refer to it.
    ///
    /// The rule is a dict with its text, its `#[name]` and its source position,
    /// or `None` if no rule has that id.
    fn rule(&self, py: Python<'_>, id: usize) -> PyResult<Py<PyAny>> {
        let inner = self.lock()?;
        match inner.engine.rule(id) {
            Some(rule) => into_python(py, &rule),
            None => Ok(py.None()),
        }
    }

    /// Returns the stored rows of a predicate, as a list of tuples of converted values.
    ///
    /// For large results, iterate over `cursor` instead.
    /// Raises `DiagnosticsError` for a predicate that the program does not have,
    /// or that the engine does not store whole,
    /// and `ValueError` for a map whose keys convert to the same dict key.
    fn rows(&self, py: Python<'_>, predicate: &str) -> PyResult<Vec<Py<PyAny>>> {
        let mut inner = self.lock()?;
        let rows = inner
            .engine
            .rows(predicate)
            .map_err(|diagnostic| inner.raise(py, diagnostic))?;
        rows.into_iter()
            .map(|row| {
                let values: Vec<Py<PyAny>> = row
                    .iter()
                    .map(|value| to_python(py, value.as_ref()))
                    .collect::<PyResult<_>>()?;
                Ok(PyTuple::new(py, values)?.unbind().into_any())
            })
            .collect()
    }

    /// Returns an iterator over a predicate's rows, read in bounded chunks,
    /// so that memory stays bounded however large the predicate is.
    ///
    /// Raises `DiagnosticsError` as `rows` does.
    fn cursor(slf: Py<Self>, py: Python<'_>, predicate: &str) -> PyResult<Cursor> {
        let cursor = {
            let engine = slf.get();
            let inner = engine.lock()?;
            inner
                .engine
                .cursor(predicate)
                .map_err(|diagnostic| inner.raise(py, diagnostic))?
        };
        Ok(Cursor {
            engine: slf,
            cursor: Mutex::new((cursor, VecDeque::new())),
        })
    }

    /// Returns the number of stored rows of a predicate.
    ///
    /// Raises `DiagnosticsError` as `rows` does.
    fn count(&self, py: Python<'_>, predicate: &str) -> PyResult<usize> {
        let inner = self.lock()?;
        inner
            .engine
            .count(predicate)
            .map_err(|diagnostic| inner.raise(py, diagnostic))
    }

    /// Returns every predicate of the program, sorted by name,
    /// as a tuple of its name, its number of columns and its role.
    ///
    /// The role is `"derived"` for a predicate that rules derive and that has no input facts,
    /// `"both"` for one that rules derive and that also has input facts,
    /// and `"extensional"` for any other.
    fn predicates(&self) -> PyResult<Vec<(String, usize, &'static str)>> {
        Ok(self
            .lock()?
            .engine
            .predicates()
            .into_iter()
            .map(|info| (info.name, info.arity, info.role.name()))
            .collect())
    }

    /// Returns the inferred column types of a predicate, in declaration syntax.
    ///
    /// Returns `None` for a predicate that the program never used,
    /// or that the engine does not store whole.
    fn column_types(&self, predicate: &str) -> PyResult<Option<Vec<String>>> {
        Ok(self.lock()?.engine.column_types(predicate).map(|types| {
            types
                .iter()
                .map(|datatype| TypeSyntax::of(datatype).to_string())
                .collect()
        }))
    }

    /// Writes the program's exports into `directory`.
    ///
    /// Raises `DiagnosticsError` if no program is loaded, or if a file cannot be written.
    fn export(&self, py: Python<'_>, directory: &str) -> PyResult<()> {
        let mut inner = self.lock()?;
        inner
            .engine
            .export(directory)
            .map_err(|diagnostic| inner.raise(py, diagnostic))
    }

    /// Returns a predicate's rows as an Arrow stream.
    ///
    /// The stream implements the Arrow C stream interface (`__arrow_c_stream__`),
    /// so libraries that accept it read it directly,
    /// as in `polars.DataFrame(stream)` or `pyarrow.table(stream)`.
    /// It needs no pyarrow.
    /// A column whose values are all integers, all doubles or all booleans becomes an Arrow
    /// array of that type, and every other column becomes text.
    /// Raises `DiagnosticsError` as `rows` does.
    #[cfg(feature = "arrow")]
    fn arrow_stream(&self, py: Python<'_>, predicate: &str) -> PyResult<crate::arrow::ArrowStream> {
        let mut inner = self.lock()?;
        let inner = &mut *inner;
        crate::arrow::stream(py, &mut inner.engine, &inner.sources, predicate)
    }

    /// Returns a predicate's rows as a `pyarrow.Table`, which needs pyarrow.
    #[cfg(feature = "arrow")]
    fn to_arrow(&self, py: Python<'_>, predicate: &str) -> PyResult<Py<PyAny>> {
        let stream = self.arrow_stream(py, predicate)?;
        let table = py.import("pyarrow")?.getattr("table")?;
        Ok(table.call1((stream,))?.unbind())
    }

    /// Returns a predicate's rows as a `pandas.DataFrame`, which needs pyarrow and pandas.
    #[cfg(feature = "arrow")]
    fn to_pandas(&self, py: Python<'_>, predicate: &str) -> PyResult<Py<PyAny>> {
        let table = self.to_arrow(py, predicate)?;
        Ok(table.bind(py).call_method0("to_pandas")?.unbind())
    }

    /// Returns the reports, such as warnings, that the engine has collected while loading and
    /// reasoning.
    fn reports(&self) -> PyResult<Vec<Diagnostic>> {
        let inner = self.lock()?;
        Ok(inner
            .engine
            .reports()
            .iter()
            .map(|diagnostic| resolve(diagnostic, &inner.sources))
            .collect())
    }

    /// Returns the number of input facts: the rows of the program's facts and imports,
    /// and the rows added through `import_rows`.
    #[getter]
    fn loaded_facts(&self) -> PyResult<usize> {
        Ok(self.lock()?.engine.loaded_facts())
    }

    /// Returns the number of facts that reasoning stored,
    /// including the rows that bound imports fetched.
    #[getter]
    fn derived_facts(&self) -> PyResult<usize> {
        Ok(self.lock()?.engine.derived_facts())
    }

    /// Returns the number of rule applications performed so far.
    #[getter]
    fn applications(&self) -> PyResult<u64> {
        Ok(self.lock()?.engine.applications())
    }

    /// Returns the number of bytes that the tables and the dictionary currently use.
    #[getter]
    fn memory_bytes(&self) -> PyResult<usize> {
        Ok(self.lock()?.engine.memory_bytes())
    }

    /// Returns the wall-clock time that reasoning spent in each phase, in nanoseconds,
    /// under the keys `apply`, `store` and `consolidate`.
    fn timings(&self) -> PyResult<std::collections::HashMap<&'static str, u64>> {
        let timings = self.lock()?.engine.timings();
        Ok(std::collections::HashMap::from([
            ("apply", timings.apply),
            ("store", timings.store),
            ("consolidate", timings.consolidate),
        ]))
    }

    /// Returns the engine as `Engine(N predicates, M facts)`,
    /// or as `Engine(busy)` while a method runs.
    fn __repr__(&self) -> String {
        match self.inner.try_lock() {
            Ok(inner) => format!(
                "Engine({} predicates, {} facts)",
                inner.engine.predicates().len(),
                inner.engine.loaded_facts() + inner.engine.derived_facts()
            ),
            Err(_) => String::from("Engine(busy)"),
        }
    }
}

/// An iterator over a predicate's rows, which reads bounded chunks from its engine as it
/// advances.
#[pyclass(frozen, module = "nmo")]
#[derive(Debug)]
pub struct Cursor {
    /// The engine whose rows the cursor reads.
    engine: Py<Engine>,
    /// The engine's cursor, and the rows read but not yet returned.
    cursor: Mutex<(PredicateCursor, VecDeque<Vec<DataValue>>)>,
}

#[pymethods]
impl Cursor {
    /// Returns the number of rows not yet returned.
    ///
    /// Raises `NemoError` while another thread uses the cursor.
    #[getter]
    fn remaining(&self) -> PyResult<usize> {
        let state = self
            .cursor
            .try_lock()
            .map_err(|_| NemoError::new_err("the cursor is busy in another thread"))?;
        Ok(state.0.remaining() + state.1.len())
    }

    /// Returns the cursor itself.
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Returns the next row as a tuple of converted values,
    /// reading the next chunk from the engine when the rows read so far are used up.
    ///
    /// Raises `NemoError` while another thread uses the cursor or the engine,
    /// and `DiagnosticsError` if the engine cannot read the chunk.
    fn __next__(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        let mut state = self
            .cursor
            .try_lock()
            .map_err(|_| NemoError::new_err("the cursor is busy in another thread"))?;
        let (cursor, buffered) = &mut *state;
        if buffered.is_empty() && !cursor.is_done() {
            let mut inner = self.engine.get().lock()?;
            let chunk = inner
                .engine
                .read_chunk(cursor, PredicateCursor::BUDGET)
                .map_err(|diagnostic| inner.raise(py, diagnostic))?;
            buffered.extend(chunk.rows());
        }
        let Some(row) = buffered.pop_front() else {
            return Ok(None);
        };
        let values: Vec<Py<PyAny>> = row
            .iter()
            .map(|value| to_python(py, value.as_ref()))
            .collect::<PyResult<_>>()?;
        Ok(Some(PyTuple::new(py, values)?.unbind().into_any()))
    }
}

/// Returns the facts of a `trace` call, given as one fact string or as a sequence of them.
///
/// Each is parsed with its terminating `.` optional,
/// and a text that is not a fact raises `DiagnosticsError`.
fn trace_facts(
    py: Python<'_>,
    facts: &Bound<'_, PyAny>,
    inner: &Inner,
) -> PyResult<Vec<nemo::model::Fact>> {
    let texts: Vec<String> = match facts.extract::<String>() {
        Ok(text) => vec![text],
        Err(_) => facts.extract()?,
    };
    texts
        .iter()
        .map(|text| {
            nemo::model::Fact::parse_loose(text).map_err(|diagnostic| inner.raise(py, diagnostic))
        })
        .collect()
}

/// Returns a protocol value made from Python objects, converted through their JSON form.
///
/// Raises `NemoError` if the JSON does not have the value's shape.
fn from_python<T: serde::de::DeserializeOwned>(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
) -> PyResult<T> {
    let json = py
        .import("json")?
        .getattr("dumps")?
        .call1((value,))?
        .extract::<String>()?;
    serde_json::from_str(&json)
        .map_err(|error| NemoError::new_err(format!("invalid query: {error}")))
}

/// Replaces the ids in the Python value of a trace response with the dicts they refer to.
///
/// A node's `rule` becomes the rule's dict, and its `premises` become the premises' nodes.
/// A traced fact's `node` becomes the node's dict.
fn linked(py: Python<'_>, response: &TraceResponse, value: &Bound<'_, PyAny>) -> PyResult<()> {
    let nodes = value.get_item("nodes")?.cast_into::<PyList>()?;
    let rules = value.get_item("rules")?.cast_into::<PyList>()?;
    for (node, linked) in response.nodes.iter().zip(nodes.iter()) {
        let Some(derivation) = &node.derivation else {
            continue;
        };
        linked.set_item("rule", rules.get_item(derivation.rule)?)?;
        let premises = derivation
            .premises
            .iter()
            .map(|&premise| nodes.get_item(premise))
            .collect::<PyResult<Vec<_>>>()?;
        linked.set_item("premises", PyList::new(py, premises)?)?;
    }
    for (traced, linked) in response
        .facts
        .iter()
        .zip(value.get_item("facts")?.try_iter()?)
    {
        if let Some(node) = traced.node {
            linked?.set_item("node", nodes.get_item(node)?)?;
        }
    }
    Ok(())
}

/// Returns a protocol value as Python objects, through its JSON form,
/// with the protocol's keys unchanged.
fn into_python<T: serde::Serialize>(py: Python<'_>, value: &T) -> PyResult<Py<PyAny>> {
    let json =
        serde_json::to_string(value).map_err(|error| NemoError::new_err(error.to_string()))?;
    Ok(py
        .import("json")?
        .getattr("loads")?
        .call1((json,))?
        .unbind())
}
