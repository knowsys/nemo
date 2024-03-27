use std::{
    collections::{HashMap, HashSet},
    fs::read_to_string,
};

use nemo::{
    datavalues::{AnyDataValue, DataValue},
    execution::{tracing::trace::ExecutionTraceTree, ExecutionEngine},
    io::{resource_providers::ResourceProviders, ExportManager, ImportManager},
    model::{
        chase_model::{ChaseAtom, ChaseFact},
        ExportDirective, Identifier, Variable,
    },
};

use pyo3::{create_exception, exceptions::PyNotImplementedError, prelude::*, types::PyDict};

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
        self.map_err(|err| NemoError::new_err(format!("{}", err)))
    }
}

#[pyclass]
#[derive(Clone)]
struct NemoProgram(nemo::model::Program);

#[pyfunction]
fn load_file(file: String) -> PyResult<NemoProgram> {
    let contents = read_to_string(file)?;
    let program = nemo::io::parser::parse_program(contents).py_res()?;
    Ok(NemoProgram(program))
}

#[pyfunction]
fn load_string(rules: String) -> PyResult<NemoProgram> {
    let program = nemo::io::parser::parse_program(rules).py_res()?;
    Ok(NemoProgram(program))
}

#[pymethods]
impl NemoProgram {
    fn output_predicates(&self) -> Vec<String> {
        self.0.output_predicates().map(|id| id.name()).collect()
    }

    fn edb_predicates(&self) -> HashSet<String> {
        self.0
            .edb_predicates()
            .into_iter()
            .map(|id| id.name())
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
        let export_manager = ExportManager::new()
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
struct NemoResults(Box<dyn Iterator<Item = Vec<AnyDataValue>> + Send>);

fn datavalue_to_python(py: Python<'_>, v: AnyDataValue) -> PyResult<&PyAny> {
    match v.value_domain() {
        nemo::datavalues::ValueDomain::LanguageTaggedString => {
            let (value, tag) = v.to_language_tagged_string_unchecked();
            let lit = NemoLiteral {
                value,
                language: Some(tag),
                datatype: RDF_LANG_STRING.to_string(),
            };
            Ok(Py::new(py, lit)?.to_object(py).into_ref(py))
        }
        nemo::datavalues::ValueDomain::PlainString | nemo::datavalues::ValueDomain::Iri => {
            Ok(v.canonical_string().into_py(py).into_ref(py))
        }
        nemo::datavalues::ValueDomain::Double => Ok(v.to_f64_unchecked().into_py(py).into_ref(py)),
        nemo::datavalues::ValueDomain::Float => Ok(v.to_f32_unchecked().into_py(py).into_ref(py)),
        nemo::datavalues::ValueDomain::NonNegativeLong
        | nemo::datavalues::ValueDomain::UnsignedInt
        | nemo::datavalues::ValueDomain::NonNegativeInt
        | nemo::datavalues::ValueDomain::Long
        | nemo::datavalues::ValueDomain::Int => Ok(v.to_i64_unchecked().into_py(py).into_ref(py)),
        nemo::datavalues::ValueDomain::Boolean => {
            Ok(v.to_boolean_unchecked().into_py(py).into_ref(py))
        }
        nemo::datavalues::ValueDomain::Null => Ok(v
            .to_null_unchecked()
            .canonical_string()
            .into_py(py)
            .into_ref(py)),
        nemo::datavalues::ValueDomain::Tuple => todo!("tuples are not supported yet"),
        nemo::datavalues::ValueDomain::Map => todo!("maps are not supported yet"),
        nemo::datavalues::ValueDomain::UnsignedLong | nemo::datavalues::ValueDomain::Other => {
            let lit = NemoLiteral {
                value: v.lexical_value(),
                language: None,
                datatype: v.datatype_iri(),
            };
            Ok(Py::new(py, lit)?.to_object(py).into_ref(py))
        }
    }
}

#[pyclass]
struct NemoFact(ChaseFact);

#[pymethods]
impl NemoFact {
    fn predicate(&self) -> String {
        self.0.predicate().to_string()
    }

    fn constants<'a>(&self, py: Python<'a>) -> PyResult<Vec<&'a PyAny>> {
        self.0
            .terms()
            .iter()
            .map(|c| datavalue_to_python(py, c.clone()))
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

    fn assignement(&self, py: Python<'_>) -> PyResult<Option<PyObject>> {
        match &self.0 {
            ExecutionTraceTree::Fact(_) => Ok(None),
            ExecutionTraceTree::Rule(application, _) => {
                Ok(Some(assignement_to_dict(&application.assignment, py)?))
            }
        }
    }

    fn dict(&self, py: Python) -> PyResult<PyObject> {
        trace_to_dict(&self.0, py)
    }
}

fn assignement_to_dict(
    assignment: &HashMap<Variable, AnyDataValue>,
    py: Python,
) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    for (variable, value) in assignment {
        dict.set_item(
            variable.to_string(),
            datavalue_to_python(py, value.clone())?,
        )?;
    }

    Ok(dict.to_object(py))
}

fn trace_to_dict(trace: &ExecutionTraceTree, py: Python) -> PyResult<PyObject> {
    let result = PyDict::new(py);
    match &trace {
        ExecutionTraceTree::Fact(fact) => result.set_item("fact", fact.to_string())?,
        ExecutionTraceTree::Rule(rule_application, subtraces) => {
            result.set_item("rule", rule_application.rule.to_string())?;
            result.set_item(
                "assignment",
                assignement_to_dict(&rule_application.assignment, py)?,
            )?;
            let subtraces: Vec<_> = subtraces
                .iter()
                .map(|trace| trace_to_dict(trace, py))
                .collect::<PyResult<_>>()?;
            result.set_item("subtraces", subtraces)?;
        }
    };
    Ok(result.to_object(py))
}

#[pymethods]
impl NemoResults {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>) -> PyResult<Option<Vec<&PyAny>>> {
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
    program: NemoProgram,
    engine: nemo::execution::DefaultExecutionEngine,
}

#[pymethods]
impl NemoEngine {
    #[new]
    fn py_new(program: NemoProgram) -> PyResult<Self> {
        let import_manager = ImportManager::new(ResourceProviders::default());
        let engine = ExecutionEngine::initialize(&program.0, import_manager).py_res()?;
        Ok(NemoEngine { program, engine })
    }

    fn reason(&mut self) -> PyResult<()> {
        self.engine.execute().py_res()?;
        Ok(())
    }

    fn trace(&mut self, fact: String) -> Option<NemoTrace> {
        let parsed_fact = nemo::io::parser::parse_fact(fact).py_res().ok()?;
        let (trace, handles) = self.engine.trace(self.program.0.clone(), vec![parsed_fact]);
        let handle = *handles
            .first()
            .expect("Function trace always returns a handle for each input fact");

        trace.tree(handle).map(NemoTrace)
    }

    fn write_result(
        &mut self,
        predicate: String,
        output_manager: &PyCell<NemoOutputManager>,
    ) -> PyResult<()> {
        let identifier = Identifier::from(predicate);

        let Some(arity) = self.engine.predicate_arity(&identifier) else {
            return Ok(());
        };

        output_manager
            .borrow()
            .0
            .export_table(
                &ExportDirective::default(identifier.clone()),
                self.engine.predicate_rows(&identifier).py_res()?,
                arity,
            )
            .py_res()?;

        Ok(())
    }

    fn result(mut slf: PyRefMut<'_, Self>, predicate: String) -> PyResult<Py<NemoResults>> {
        let iter = slf
            .engine
            .predicate_rows(&Identifier::from(predicate))
            .py_res()?;
        let results = NemoResults(Box::new(
            iter.into_iter().flatten().collect::<Vec<_>>().into_iter(),
        ));

        Py::new(slf.py(), results)
    }
}

/// Python bindings for the nemo reasoner
#[pymodule]
fn nmo_python(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<NemoProgram>()?;
    m.add_class::<NemoEngine>()?;
    m.add_class::<NemoResults>()?;
    m.add_class::<NemoOutputManager>()?;
    m.add_class::<NemoLiteral>()?;
    m.add_function(wrap_pyfunction!(load_file, m)?)?;
    m.add_function(wrap_pyfunction!(load_string, m)?)?;
    Ok(())
}
