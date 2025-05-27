// FIXME: remove this once the pyo3 macros don't trigger this
#![allow(non_local_definitions)]

use std::{collections::HashSet, fs::read_to_string, time::Duration};

use nemo::{
    api::load_program,
    chase_model::ChaseAtom,
    datavalues::{AnyDataValue, DataValue},
    error::Error,
    execution::{tracing::trace::ExecutionTraceTree, ExecutionEngine},
    io::{resource_providers::ResourceProviders, ExportManager, ImportManager},
    meta::timing::TimedCode,
    rule_model::{
        components::{
            fact::Fact,
            tag::Tag,
            term::{primitive::Primitive, Term},
            ComponentBehavior,
        },
        program::ProgramRead,
        substitution::Substitution,
    },
};

use pyo3::{
    create_exception, exceptions::PyNotImplementedError, prelude::*, types::PyDict, IntoPyObjectExt,
};

create_exception!(module, NemoError, pyo3::exceptions::PyException);

pub const RDF_LANG_STRING: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString";
pub const XSD_STRING: &str = "http://www.w3.org/2001/XMLSchema#string";

trait PythonResult {
    type Value;

    fn py_res(self) -> PyResult<Self::Value>;
}

impl<T> PythonResult for Result<T, nemo::error::Error> {
    type Value = T;

    fn py_res(self) -> PyResult<Self::Value> {
        self.map_err(|err| NemoError::new_err(format!("{err}")))
    }
}
impl<T> PythonResult for (T, Vec<Error>) {
    type Value = T;

    fn py_res(self) -> PyResult<Self::Value> {
        todo!("It is unclear what should get returned")
    }
}

#[pyclass]
#[derive(Clone)]
struct NemoProgram(nemo::rule_model::program::Program);

#[pyfunction]
fn load_file(file: String) -> PyResult<NemoProgram> {
    let contents = read_to_string(file)?;
    load_string(contents)
}

#[pyfunction]
fn load_string(rules: String) -> PyResult<NemoProgram> {
    let program = load_program(rules, String::default())
        .map_err(Error::ProgramReport)
        .py_res()?;

    Ok(NemoProgram(program))
}

#[pymethods]
impl NemoProgram {
    fn output_predicates(&self) -> Vec<String> {
        self.0
            .outputs()
            .map(|output| output.predicate().to_string())
            .collect()
    }

    fn edb_predicates(&self) -> HashSet<String> {
        self.0
            .import_predicates()
            .into_iter()
            .map(|predicate| predicate.to_string())
            .collect()
    }
}

#[pyclass]
struct NemoOutputManager(nemo::io::ExportManager);

#[pymethods]
impl NemoOutputManager {
    #[new]
    #[pyo3(signature=(path, overwrite=false, gzip=false))]
    fn py_new(path: String, overwrite: bool, gzip: bool) -> PyResult<Self> {
        let export_manager = ExportManager::default()
            .set_base_path(path.into())
            .overwrite(overwrite)
            .compress(gzip);
        Ok(NemoOutputManager(export_manager))
    }
}

#[pyclass]
#[derive(Debug, PartialEq, Eq)]
struct NemoLiteral {
    value: String,
    language: Option<String>,
    datatype: String,
}

#[pymethods]
impl NemoLiteral {
    #[new]
    #[pyo3(signature=(value, lang=None))]
    fn new(value: PyObject, lang: Option<String>) -> PyResult<NemoLiteral> {
        Python::with_gil(|py| {
            let inner: String = value.extract(py).map_err(|_| {
                NemoError::new_err("Only string arguments are currently supported".to_string())
            })?;

            let datatype = if lang.is_some() {
                RDF_LANG_STRING.to_string()
            } else {
                XSD_STRING.to_string()
            };

            Ok(NemoLiteral {
                value: inner,
                language: lang,
                datatype,
            })
        })
    }

    fn value(&self) -> &str {
        &self.value
    }

    fn datatype(&self) -> &str {
        &self.datatype
    }

    fn language(&self) -> Option<&String> {
        self.language.as_ref()
    }

    fn __richcmp__(&self, other: &Self, op: pyo3::basic::CompareOp) -> PyResult<bool> {
        match op {
            pyo3::pyclass::CompareOp::Eq => Ok(self == other),
            pyo3::pyclass::CompareOp::Ne => Ok(self != other),
            _ => Err(PyNotImplementedError::new_err(
                "RDF comparison is not implemented",
            )),
        }
    }
}

#[pyclass]
struct NemoResults(Box<dyn Iterator<Item = Vec<AnyDataValue>> + Send + Sync>);

fn datavalue_to_python(py: Python<'_>, v: AnyDataValue) -> PyResult<Bound<PyAny>> {
    match v.value_domain() {
        nemo::datavalues::ValueDomain::LanguageTaggedString => {
            let (value, tag) = v.to_language_tagged_string_unchecked();
            let lit = NemoLiteral {
                value,
                language: Some(tag),
                datatype: RDF_LANG_STRING.to_string(),
            };
            Py::new(py, lit)?.into_bound_py_any(py)
        }
        nemo::datavalues::ValueDomain::PlainString | nemo::datavalues::ValueDomain::Iri => {
            v.canonical_string().into_bound_py_any(py)
        }
        nemo::datavalues::ValueDomain::Double => v.to_f64_unchecked().into_bound_py_any(py),
        nemo::datavalues::ValueDomain::Float => v.to_f32_unchecked().into_bound_py_any(py),
        nemo::datavalues::ValueDomain::NonNegativeLong
        | nemo::datavalues::ValueDomain::UnsignedInt
        | nemo::datavalues::ValueDomain::NonNegativeInt
        | nemo::datavalues::ValueDomain::Long
        | nemo::datavalues::ValueDomain::Int => v.to_i64_unchecked().into_bound_py_any(py),
        nemo::datavalues::ValueDomain::Boolean => v.to_boolean_unchecked().into_bound_py_any(py),
        nemo::datavalues::ValueDomain::Null => v
            .to_null_unchecked()
            .canonical_string()
            .into_bound_py_any(py),
        nemo::datavalues::ValueDomain::Tuple => todo!("tuples are not supported yet"),
        nemo::datavalues::ValueDomain::Map => todo!("maps are not supported yet"),
        nemo::datavalues::ValueDomain::UnsignedLong | nemo::datavalues::ValueDomain::Other => {
            let lit = NemoLiteral {
                value: v.lexical_value(),
                language: None,
                datatype: v.datatype_iri(),
            };
            Py::new(py, lit)?.into_bound_py_any(py)
        }
    }
}

#[pyclass]
struct NemoFact(nemo::chase_model::GroundAtom);

#[pymethods]
impl NemoFact {
    fn predicate(&self) -> String {
        self.0.predicate().to_string()
    }

    fn constants<'a>(&self, py: Python<'a>) -> PyResult<Vec<Bound<'a, PyAny>>> {
        self.0
            .terms()
            .map(|c| datavalue_to_python(py, c.value()))
            .collect()
    }

    fn __repr__(&self) -> String {
        self.0.to_string()
    }
}

#[pyclass]
struct NemoTrace(ExecutionTraceTree);

#[pymethods]
impl NemoTrace {
    fn subtraces(&self) -> Option<Vec<NemoTrace>> {
        match &self.0 {
            ExecutionTraceTree::Fact(_) => None,
            ExecutionTraceTree::Rule(_, subtraces) => {
                Some(subtraces.iter().map(|t| NemoTrace(t.clone())).collect())
            }
        }
    }

    fn fact(&self) -> Option<NemoFact> {
        match &self.0 {
            ExecutionTraceTree::Fact(f) => Some(NemoFact(f.clone())),
            ExecutionTraceTree::Rule(_, _) => None,
        }
    }

    fn rule(&self) -> Option<String> {
        match &self.0 {
            ExecutionTraceTree::Fact(_) => None,
            ExecutionTraceTree::Rule(application, _) => Some(application.rule.to_string()),
        }
    }

    fn assignement<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyDict>>> {
        match &self.0 {
            ExecutionTraceTree::Fact(_) => Ok(None),
            ExecutionTraceTree::Rule(application, _) => {
                Ok(Some(assignement_to_dict(&application.assignment, py)?))
            }
        }
    }

    fn dict<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        trace_to_dict(&self.0, py)
    }
}

fn assignement_to_dict<'py>(
    assignment: &Substitution,
    py: Python<'py>,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    for (variable, term) in assignment {
        if let Term::Primitive(Primitive::Ground(ground)) = term {
            dict.set_item(
                variable.to_string(),
                datavalue_to_python(py, ground.value())?,
            )?;
        }
    }

    Ok(dict.into_pyobject(py)?)
}

fn trace_to_dict<'py>(trace: &ExecutionTraceTree, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
    let result = PyDict::new(py);
    match &trace {
        ExecutionTraceTree::Fact(fact) => result.set_item("fact", fact.to_string())?,
        ExecutionTraceTree::Rule(rule_application, subtraces) => {
            result.set_item("rule", rule_application.rule.to_string())?;
            result.set_item(
                "assignment",
                assignement_to_dict(&rule_application.assignment, py)?,
            )?;
            if let Some(name) = rule_application.rule.name() {
                result.set_item("name", name)?;
            }
            if let Some(display) = rule_application
                .rule
                .instantiated_display(&rule_application.assignment)
            {
                result.set_item("display", display)?;
            }

            let subtraces: Vec<_> = subtraces
                .iter()
                .map(|trace| trace_to_dict(trace, py))
                .collect::<PyResult<_>>()?;
            result.set_item("subtraces", subtraces)?;
        }
    };
    Ok(result.into_pyobject(py)?)
}

#[pymethods]
impl NemoResults {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>) -> PyResult<Option<Vec<Bound<PyAny>>>> {
        let Some(next) = slf.0.next() else {
            return Ok(None);
        };

        Ok(Some(
            next.into_iter()
                .map(|v| datavalue_to_python(slf.py(), v))
                .collect::<Result<_, _>>()?,
        ))
    }
}

#[pyclass(unsendable)]
struct NemoEngine {
    engine: nemo::execution::DefaultExecutionEngine,
}

#[pyclass]
#[derive(Debug, Clone)]
struct NemoTiming {
    #[pyo3(get)]
    name: String,
    inner: TimedCode,
}

#[pymethods]
impl NemoTiming {
    #[getter]
    fn system_time(&self) -> Duration {
        self.inner.timings().system_time()
    }

    #[getter]
    fn process_time(&self) -> Duration {
        self.inner.timings().process_time()
    }

    #[getter]
    fn thread_time(&self) -> Duration {
        self.inner.timings().thread_time()
    }

    #[getter]
    fn subnodes(&self) -> Vec<NemoTiming> {
        self.inner
            .sub_nodes()
            .map(|(name, inner)| NemoTiming {
                name: name.to_string(),
                inner: inner.clone(),
            })
            .collect()
    }

    fn subnode(&self, name: &str) -> Option<NemoTiming> {
        let parts: Vec<_> = name.split('/').collect();
        let mut node = &self.inner;

        for part in &parts {
            let (_, inner) = node.sub_nodes().find(|(name, _)| name == part)?;
            node = inner;
        }

        let name = parts.last()?.to_string();

        Some(NemoTiming {
            name,
            inner: node.clone(),
        })
    }

    fn __repr__(&self) -> String {
        format!(
            "NemoTiming(name = {:?}, process_time = {}ms, system_time = {}ms, thread_time = {}ms)",
            self.name,
            self.process_time().as_millis(),
            self.system_time().as_millis(),
            self.thread_time().as_millis()
        )
    }
}

#[pymethods]
impl NemoEngine {
    #[new]
    fn py_new(program: NemoProgram) -> PyResult<Self> {
        TimedCode::instance().reset();
        let import_manager = ImportManager::new(ResourceProviders::default());
        let engine = ExecutionEngine::initialize(program.0.clone(), import_manager).py_res()?;
        Ok(NemoEngine { engine })
    }

    fn reason(&mut self) -> PyResult<()> {
        TimedCode::instance().start();
        TimedCode::instance().sub("Reasoning").start();

        self.engine.execute().py_res()?;

        TimedCode::instance().sub("Reasoning").stop();
        TimedCode::instance().stop();
        Ok(())
    }

    fn trace(&mut self, fact_string: String) -> Option<NemoTrace> {
        let fact = Fact::parse(&fact_string).ok()?;
        fact.validate().ok()?;

        let (trace, handles) = self.engine.trace(vec![fact]).ok()?;
        let handle = *handles
            .first()
            .expect("Function trace always returns a handle for each input fact");

        trace.tree(handle).map(NemoTrace)
    }

    fn timing(&self) -> NemoTiming {
        NemoTiming {
            name: "root".into(),
            inner: TimedCode::instance().clone(),
        }
    }

    fn write_result(
        &mut self,
        predicate: String,
        output_manager: &Bound<NemoOutputManager>,
    ) -> PyResult<()> {
        let tag = Tag::new(predicate);

        let Some(_arity) = self.engine.predicate_arity(&tag) else {
            return Ok(());
        };

        let export_handler = if let Some((_, handler)) = self
            .engine
            .exports()
            .iter()
            .find(|(predicate, _)| *predicate == tag)
        {
            handler.clone()
        } else {
            return Ok(());
        };

        output_manager
            .borrow()
            .0
            .export_table(
                &tag,
                &export_handler,
                self.engine.predicate_rows(&tag).py_res()?,
            )
            .py_res()?;

        Ok(())
    }

    fn result(mut slf: PyRefMut<'_, Self>, predicate: String) -> PyResult<Py<NemoResults>> {
        let iter = slf.engine.predicate_rows(&Tag::new(predicate)).py_res()?;
        let results = NemoResults(Box::new(
            iter.into_iter().flatten().collect::<Vec<_>>().into_iter(),
        ));

        Py::new(slf.py(), results)
    }
}

/// Python bindings for the nemo reasoner
#[pymodule]
fn nmo_python(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_class::<NemoProgram>()?;
    m.add_class::<NemoEngine>()?;
    m.add_class::<NemoResults>()?;
    m.add_class::<NemoOutputManager>()?;
    m.add_class::<NemoLiteral>()?;
    m.add_function(wrap_pyfunction!(load_file, m)?)?;
    m.add_function(wrap_pyfunction!(load_string, m)?)?;
    Ok(())
}
