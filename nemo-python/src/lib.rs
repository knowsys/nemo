use std::{
    collections::{HashMap, HashSet},
    fs::read_to_string,
};

use nemo::{
    datatypes::Double,
    execution::{tracing::trace::ExecutionTraceTree, ExecutionEngine},
    io::{resource_providers::ResourceProviders, OutputFileManager, RecordWriter},
    model::{
        chase_model::{ChaseAtom, ChaseFact},
        types::primitive_logical_value::PrimitiveLogicalValueT,
        Constant, NumericLiteral, RdfLiteral, Variable, XSD_STRING,
    },
};

use pyo3::{create_exception, exceptions::PyNotImplementedError, prelude::*, types::PyDict};

create_exception!(module, NemoError, pyo3::exceptions::PyException);

pub const RDF_LANG_STRING: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString";

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
struct NemoOutputManager(nemo::io::OutputFileManager);

#[pymethods]
impl NemoOutputManager {
    #[new]
    #[pyo3(signature=(path, overwrite=false, gzip=false))]
    fn py_new(path: String, overwrite: bool, gzip: bool) -> PyResult<Self> {
        let output_manager = OutputFileManager::try_new(path.into(), overwrite, gzip).py_res()?;

        Ok(NemoOutputManager(output_manager))
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
struct NemoResults(Box<dyn Iterator<Item = Vec<PrimitiveLogicalValueT>> + Send>);

fn constant_to_python<'a>(py: Python<'a>, v: &Constant) -> PyResult<&'a PyAny> {
    let decimal = py.import("decimal")?.getattr("Decimal")?;
    match v {
        Constant::Abstract(c) => Ok(c.to_string().into_py(py).into_ref(py)),
        Constant::NumericLiteral(NumericLiteral::Integer(i)) => Ok(i.into_py(py).into_ref(py)),
        Constant::NumericLiteral(NumericLiteral::Double(d)) => {
            Ok(f64::from(*d).into_py(py).into_ref(py))
        }
        // currently we pack decimals into strings, maybe this should change
        Constant::NumericLiteral(_) => decimal.call1((v.to_string(),)),
        Constant::StringLiteral(s) => Ok(s.into_py(py).into_ref(py)),
        Constant::RdfLiteral(lit) => (|| {
            let lit = match lit {
                RdfLiteral::DatatypeValue { value, datatype } => NemoLiteral {
                    value: value.clone(),
                    language: None,
                    datatype: datatype.clone(),
                },
                RdfLiteral::LanguageString { value, tag } => NemoLiteral {
                    value: value.clone(),
                    language: Some(tag.clone()),
                    datatype: RDF_LANG_STRING.to_string(),
                },
            };
            Ok(Py::new(py, lit)?.to_object(py).into_ref(py))
        })(),
        Constant::MapLiteral(_map) => todo!("maps are not yet supported"),
    }
}

fn logical_value_to_python(py: Python<'_>, v: PrimitiveLogicalValueT) -> PyResult<&PyAny> {
    match v {
        PrimitiveLogicalValueT::Any(rdf) => constant_to_python(py, &rdf),
        PrimitiveLogicalValueT::String(s) => Ok(String::from(s).into_py(py).into_ref(py)),
        PrimitiveLogicalValueT::Integer(i) => Ok(i64::from(i).into_py(py).into_ref(py)),
        PrimitiveLogicalValueT::Float64(d) => {
            Ok(f64::from(Double::from(d)).into_py(py).into_ref(py))
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
            .map(|c| constant_to_python(py, c))
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

fn assignement_to_dict(assignment: &HashMap<Variable, Constant>, py: Python) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    for (variable, value) in assignment {
        dict.set_item(variable.to_string(), constant_to_python(py, value)?)?;
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
                .map(|v| logical_value_to_python(slf.py(), v))
                .collect::<Result<_, _>>()?,
        ))
    }
}

#[pyclass(unsendable)]
struct NemoEngine(nemo::execution::DefaultExecutionEngine);

#[pymethods]
impl NemoEngine {
    #[new]
    fn py_new(program: NemoProgram) -> PyResult<Self> {
        let engine =
            ExecutionEngine::initialize(program.0, ResourceProviders::default()).py_res()?;
        Ok(NemoEngine(engine))
    }

    fn reason(&mut self) -> PyResult<()> {
        self.0.execute().py_res()?;
        Ok(())
    }

    fn trace(&self, fact: String) -> PyResult<Option<NemoTrace>> {
        let parsed_fact = nemo::io::parser::parse_fact(fact).py_res()?;
        let (trace, handles) = self.0.trace(vec![parsed_fact]).py_res()?;
        let handle = *handles
            .first()
            .expect("Function trace always returns a handle for each input fact");

        Ok(trace.tree(handle).map(NemoTrace))
    }

    fn write_result(
        &mut self,
        predicate: String,
        output_manager: &PyCell<NemoOutputManager>,
    ) -> PyResult<()> {
        let identifier = predicate.into();
        let mut writer = output_manager
            .borrow()
            .0
            .create_file_writer(&identifier)
            .py_res()?;

        if let Some(record_iter) = self.0.output_serialization(identifier).py_res()? {
            for record in record_iter {
                writer.write_record(record).py_res()?;
            }
        }

        Ok(())
    }

    fn result(mut slf: PyRefMut<'_, Self>, predicate: String) -> PyResult<Py<NemoResults>> {
        let iter = slf.0.table_scan(predicate.into()).py_res()?;
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
