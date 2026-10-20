//! The engine: load, reason, read back.

use std::{
    collections::VecDeque,
    sync::{Mutex, MutexGuard, TryLockError},
    time::Duration,
};

use pyo3::{prelude::*, types::PyTuple};

use nemo::{
    diagnostics::render::SourceTexts,
    driver::HttpDriver,
    engine::PredicateCursor,
    values::{DataValue, TypeSyntax},
};

use crate::{
    errors::{Diagnostic, NemoError, raise, resolve},
    program::Program,
    values::{to_python, to_value},
};

/// How often a running call checks for Ctrl-C.
const SIGNAL_POLL: Duration = Duration::from_millis(50);

/// What the lock protects: the engine and the sources its diagnostics render against,
/// which change together.
#[derive(Debug, Default)]
pub(crate) struct Inner {
    /// The wrapped engine.
    pub engine: nemo::engine::Engine,
    /// The source texts of this run.
    pub sources: SourceTexts,
}

impl Inner {
    /// Raises one diagnostic against this run's sources.
    fn raise(&self, py: Python<'_>, diagnostic: nemo::diagnostics::Diagnostic) -> PyErr {
        raise(py, vec![diagnostic], &self.sources)
    }
}

/// The reasoning engine over one program:
/// load it, reason to the fixpoint, read the results back.
///
/// One call runs at a time.
/// A concurrent call from another Python thread raises `NemoError` instead of blocking,
/// since blocking under the GIL would deadlock against a run polling for Ctrl-C.
#[pyclass(frozen, module = "nmo")]
#[derive(Debug)]
pub struct Engine {
    /// The engine and sources, behind the one-call-at-a-time lock.
    inner: Mutex<Inner>,
    /// The engine's cancellation handle, outside the lock,
    /// because cancelling is how another thread reaches a *running* engine.
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
    /// Carries a program through one of the loading methods (`load`, `extend`, `reload`),
    /// fetching the data it needs from the network, and raises the errors it reports.
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
            engine.ingest(name, text, |engine, model| {
                HttpDriver::new().drive(engine, |engine| call(engine, model))
            })
        })?;
        // Linking reads the program's modules, so their sources are
        // taken in before its outcome is reported against them.
        inner.sources.absorb(inner.engine.sources());
        outcome.map_err(|diagnostics| raise(py, diagnostics, &inner.sources))
    }

    /// Runs a call with the GIL released, on a thread of its own while
    /// this one polls for signals: Ctrl-C cancels the call at its next
    /// rule boundary, or while it waits for data from the network, and
    /// raises `KeyboardInterrupt`.
    fn interruptible<T: Send>(
        &self,
        py: Python<'_>,
        call: impl FnOnce() -> T + Send,
    ) -> PyResult<T> {
        let token = self.token.clone();
        let mut interrupt: Option<PyErr> = None;
        let outcome = py.detach(|| {
            std::thread::scope(|scope| {
                let worker = scope.spawn(call);
                while !worker.is_finished() {
                    std::thread::sleep(SIGNAL_POLL);
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

    /// Returns the engine, or a raised explanation of why not now.
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

    /// Adds in-memory rows to a predicate.
    ///
    /// Before `load`, they join the program as facts.
    /// After it, they land as deltas and the next `reason()` continues to the new fixpoint,
    /// and the predicate must be one the program uses (`extend` introduces new ones).
    /// Values convert per the module's conversion table.
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

    /// Registers an in-memory resource: an `@import` whose resource
    /// name matches reads these bytes (or this text) instead of the
    /// filesystem.
    fn add_resource(&self, name: &str, content: &Bound<'_, PyAny>) -> PyResult<()> {
        let bytes: Vec<u8> = match content.extract::<String>() {
            Ok(text) => text.into_bytes(),
            Err(_) => content.extract::<Vec<u8>>()?,
        };
        self.lock()?.engine.add_resource(name, bytes);
        Ok(())
    }

    /// Registers a module text under the name a `@use` in the program writes for it,
    /// such as `lib::tc`: a `@use` of the module reads this text instead of its file.
    ///
    /// Raises `DiagnosticsError` if `name` is not a module name.
    fn add_module(&self, py: Python<'_>, name: &str, text: &str) -> PyResult<()> {
        let mut inner = self.lock()?;
        inner
            .engine
            .add_module(name, text)
            .map_err(|diagnostic| inner.raise(py, diagnostic))
    }

    /// Declares a module root: a directory whose modules a `@use` names as `name::module`.
    ///
    /// Raises `DiagnosticsError` if `name` is not a single plain name.
    fn add_module_root(&self, py: Python<'_>, name: &str, directory: &str) -> PyResult<()> {
        let mut inner = self.lock()?;
        inner
            .engine
            .add_module_root(name, directory)
            .map_err(|diagnostic| inner.raise(py, diagnostic))
    }

    /// Sets the directory relative import resources resolve against,
    /// in place of the directory of the program file an import is written in.
    fn set_import_directory(&self, directory: &str) -> PyResult<()> {
        self.lock()?.engine.set_import_directory(directory);
        Ok(())
    }

    /// Keeps every imported predicate whole.
    ///
    /// Without it, loading stores only the columns and rows of an import its rules read,
    /// and a trace or a query over the rest raises `DiagnosticsError`.
    /// Call it before `load`.
    fn retain_imports(&self) -> PyResult<()> {
        self.lock()?.engine.retain_imports();
        Ok(())
    }

    /// Loads a program: facts become stored rows, imports read their resources, rules compile.
    ///
    /// Relative resources resolve beside the program's file
    /// (see `set_import_directory`), and remote ones are fetched, with retries.
    /// Raises `DiagnosticsError` if the program does not parse or load.
    /// Ctrl-C cancels it.
    fn load(&self, py: Python<'_>, program: &Program) -> PyResult<()> {
        self.ingest(py, program, |engine, model| engine.load(model))
    }

    /// Extends the loaded program:
    /// the new statements join it, new facts and imports store, the program recompiles,
    /// and the next `reason()` continues to the new fixpoint.
    ///
    /// An extension cannot change what loaded statements mean:
    /// `@base`, prefix rebinding, and parameter redefinition raise `DiagnosticsError`,
    /// since they need a `reload`.
    fn extend(&self, py: Python<'_>, program: &Program) -> PyResult<()> {
        self.ingest(py, program, |engine, model| engine.extend(model))
    }

    /// Replaces the program:
    /// every derived row drops, every rule starts over,
    /// and the next `reason()` computes again from the loaded data.
    ///
    /// Unchanged predicates keep their data, and unchanged imports are not read again.
    /// Rows imported into a predicate whose definition changed are dropped, with a report.
    fn reload(&self, py: Python<'_>, program: &Program) -> PyResult<()> {
        self.ingest(py, program, |engine, model| engine.reload(model))
    }

    /// Checks a program without loading it:
    /// no data is read, nothing is stored, and every problem is returned rather than raised.
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

    /// Runs semi-naive evaluation to the fixpoint, fetching what bound imports ask for.
    ///
    /// The GIL is released for the whole run.
    /// Ctrl-C cancels it at a rule boundary or while it waits for data from the network,
    /// raising `KeyboardInterrupt`, and calling `reason()` again continues.
    fn reason(&self, py: Python<'_>) -> PyResult<()> {
        let mut inner = self.lock()?;
        let engine = &mut inner.engine;
        let outcome = self.interruptible(py, || {
            HttpDriver::new().drive(engine, nemo::engine::Engine::reason)
        })?;
        outcome.map_err(|diagnostic| inner.raise(py, diagnostic))
    }

    /// Cancels the running `reason()` call, for another thread,
    /// since the calling thread's Ctrl-C already cancels.
    ///
    /// A cancel issued before a run starts is cleared when it starts,
    /// since a token cancels the call that is running, not a future one.
    fn cancel(&self) {
        self.token.cancel();
    }

    /// Traces facts, one fact string like `"reach(a, b)"` or a list of them,
    /// returning the protocol response as a dict:
    /// per fact, whether it holds and, if it does, its proof tree.
    ///
    /// Raises `DiagnosticsError` if the engine holds updates `reason()` has not processed.
    fn trace(&self, py: Python<'_>, facts: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let mut inner = self.lock()?;
        let facts = trace_facts(py, facts, &inner)?;
        let response = inner
            .engine
            .trace(&facts)
            .map_err(|diagnostic| inner.raise(py, diagnostic))?;
        into_python(py, &response)
    }

    /// Returns the `trace` rendered as ASCII proof trees, the terminal-style convenience.
    fn trace_ascii(&self, py: Python<'_>, facts: &Bound<'_, PyAny>) -> PyResult<String> {
        let mut inner = self.lock()?;
        let facts = trace_facts(py, facts, &inner)?;
        let response = inner
            .engine
            .trace(&facts)
            .map_err(|diagnostic| inner.raise(py, diagnostic))?;
        Ok(response.ascii())
    }

    /// Answers the explanation view's `treeForTable` query: the facts it
    /// selects, their derivation expanded. Takes and returns the
    /// protocol's camelCase shapes as dicts.
    fn tree_for_table(&self, py: Python<'_>, query: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let query = from_python(py, query)?;
        let mut inner = self.lock()?;
        let response = inner
            .engine
            .tree_for_table(query)
            .map_err(|diagnostic| inner.raise(py, diagnostic))?;
        into_python(py, &response)
    }

    /// Answers the explanation view's `tableEntriesForTreeNodes` query:
    /// the facts of every node of the tree it shapes, parents before
    /// children. Takes the protocol's camelCase query as a dict and
    /// returns a list of node dicts.
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

    /// Returns the rule a statement id names, as traces and queries refer to it:
    /// a dict with its text, `#[name]` and source position, or `None`.
    fn rule(&self, py: Python<'_>, id: usize) -> PyResult<Py<PyAny>> {
        let inner = self.lock()?;
        match inner.engine.rule(id) {
            Some(rule) => into_python(py, &rule),
            None => Ok(py.None()),
        }
    }

    /// Returns the stored rows of a predicate, as a list of tuples of converted
    /// values. For large results, iterate `cursor` instead.
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
    fn count(&self, py: Python<'_>, predicate: &str) -> PyResult<usize> {
        let inner = self.lock()?;
        inner
            .engine
            .count(predicate)
            .map_err(|diagnostic| inner.raise(py, diagnostic))
    }

    /// Returns every predicate of the program, sorted by name,
    /// as its name, its number of columns, and what feeds it:
    /// `"extensional"` (given from outside), `"derived"` (by rules), or `"both"`.
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
    fn column_types(&self, predicate: &str) -> PyResult<Option<Vec<String>>> {
        Ok(self.lock()?.engine.column_types(predicate).map(|types| {
            types
                .iter()
                .map(|datatype| TypeSyntax::of(datatype).to_string())
                .collect()
        }))
    }

    /// Writes the program's declared exports into a directory.
    fn export(&self, py: Python<'_>, directory: &str) -> PyResult<()> {
        let mut inner = self.lock()?;
        inner
            .engine
            .export(directory)
            .map_err(|diagnostic| inner.raise(py, diagnostic))
    }

    /// Returns the predicate's rows as an Arrow stream,
    /// an object any consumer of the Arrow C stream interface (`__arrow_c_stream__`) takes
    /// directly: `polars.DataFrame(stream)`, `pyarrow.table(stream)`.
    ///
    /// It needs no pyarrow.
    /// The module notes describe the dataframe projection's typing.
    #[cfg(feature = "arrow")]
    fn arrow_stream(&self, py: Python<'_>, predicate: &str) -> PyResult<crate::arrow::ArrowStream> {
        let mut inner = self.lock()?;
        let inner = &mut *inner;
        crate::arrow::stream(py, &mut inner.engine, &inner.sources, predicate)
    }

    /// Returns the predicate's rows as a `pyarrow.Table` (pyarrow required).
    #[cfg(feature = "arrow")]
    fn to_arrow(&self, py: Python<'_>, predicate: &str) -> PyResult<Py<PyAny>> {
        let stream = self.arrow_stream(py, predicate)?;
        let table = py.import("pyarrow")?.getattr("table")?;
        Ok(table.call1((stream,))?.unbind())
    }

    /// Returns the predicate's rows as a `pandas.DataFrame` (pyarrow and pandas
    /// required).
    #[cfg(feature = "arrow")]
    fn to_pandas(&self, py: Python<'_>, predicate: &str) -> PyResult<Py<PyAny>> {
        let table = self.to_arrow(py, predicate)?;
        Ok(table.bind(py).call_method0("to_pandas")?.unbind())
    }

    /// Returns the warnings (and other reports) the run collected so far.
    fn reports(&self) -> PyResult<Vec<Diagnostic>> {
        let inner = self.lock()?;
        Ok(inner
            .engine
            .reports()
            .iter()
            .map(|diagnostic| resolve(diagnostic, &inner.sources))
            .collect())
    }

    /// Facts the program's predicates hold from outside: its facts and
    /// imports, and the rows added through `import_rows`.
    #[getter]
    fn loaded_facts(&self) -> PyResult<usize> {
        Ok(self.lock()?.engine.loaded_facts())
    }

    /// Facts the program's predicates hold from reasoning.
    #[getter]
    fn derived_facts(&self) -> PyResult<usize> {
        Ok(self.lock()?.engine.derived_facts())
    }

    /// Returns the number of rule applications performed so far.
    #[getter]
    fn applications(&self) -> PyResult<u64> {
        Ok(self.lock()?.engine.applications())
    }

    /// Bytes currently allocated across tables and the dictionary.
    #[getter]
    fn memory_bytes(&self) -> PyResult<usize> {
        Ok(self.lock()?.engine.memory_bytes())
    }

    /// Returns the wall-clock accounting of the reasoning, in nanoseconds by stage.
    fn timings(&self) -> PyResult<std::collections::HashMap<&'static str, u64>> {
        let timings = self.lock()?.engine.timings();
        Ok(std::collections::HashMap::from([
            ("apply", timings.apply),
            ("store", timings.store),
            ("consolidate", timings.consolidate),
        ]))
    }

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

/// An iterator over one predicate's rows, fetching bounded chunks from
/// its engine as it is advanced.
#[pyclass(frozen, module = "nmo")]
#[derive(Debug)]
pub struct Cursor {
    /// The engine fetched from.
    engine: Py<Engine>,
    /// The engine-side cursor and the rows fetched but not delivered.
    cursor: Mutex<(PredicateCursor, VecDeque<Vec<DataValue>>)>,
}

#[pymethods]
impl Cursor {
    /// Returns the rows not yet delivered.
    #[getter]
    fn remaining(&self) -> PyResult<usize> {
        let state = self
            .cursor
            .try_lock()
            .map_err(|_| NemoError::new_err("the cursor is busy in another thread"))?;
        Ok(state.0.remaining() + state.1.len())
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

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

/// Returns the fact list of a `trace` call: one fact string, or an iterable of
/// them, each parsed with the terminator optional.
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

/// Creates a protocol value from Python objects, through its JSON form.
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
