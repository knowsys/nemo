use std::{collections::HashSet, fs::read_to_string};

use nemo::{
    datatypes::Double,
    execution::ExecutionEngine,
    io::{resource_providers::ResourceProviders, OutputFileManager, RecordWriter},
    model::{types::primitive_logical_value::PrimitiveLogicalValueT, NumericLiteral, Term},
};

use pyo3::{create_exception, prelude::*};

create_exception!(module, NemoError, pyo3::exceptions::PyException);

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
    #[pyo3(signature =(path,overwrite=false, gzip=false))]
    fn py_new(path: String, overwrite: bool, gzip: bool) -> PyResult<Self> {
        let output_manager = OutputFileManager::try_new(path.into(), overwrite, gzip).py_res()?;

        Ok(NemoOutputManager(output_manager))
    }
}

#[pyclass]
struct NemoResults(Box<dyn Iterator<Item = Vec<PrimitiveLogicalValueT>> + Send>);

#[pymethods]
impl NemoResults {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<Vec<PyObject>> {
        let next = slf.0.next()?;

        Some(
            next.into_iter()
                .map(|v| match v {
                    PrimitiveLogicalValueT::Any(rdf) => match rdf {
                        Term::Variable(_) => panic!("Variables should not occur as results!"),
                        Term::Constant(c) => c.to_string().into_py(slf.py()),
                        Term::NumericLiteral(NumericLiteral::Integer(i)) => i.into_py(slf.py()),
                        Term::NumericLiteral(NumericLiteral::Double(d)) => {
                            f64::from(d).into_py(slf.py())
                        }
                        // currently we pack decimals into strings, maybe this should change
                        Term::NumericLiteral(_) => rdf.to_string().into_py(slf.py()),
                        Term::StringLiteral(s) => s.into_py(slf.py()),
                        Term::RdfLiteral(lit) => lit.to_string().into_py(slf.py()),
                        Term::Aggregate(_) => panic!("Aggregates should not occur as results!"),
                    },
                    PrimitiveLogicalValueT::String(s) => String::from(s).into_py(slf.py()),
                    PrimitiveLogicalValueT::Integer(i) => i64::from(i).into_py(slf.py()),
                    PrimitiveLogicalValueT::Float64(d) => {
                        f64::from(Double::from(d)).into_py(slf.py())
                    }
                })
                .collect(),
        )
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
    m.add_function(wrap_pyfunction!(load_file, m)?)?;
    m.add_function(wrap_pyfunction!(load_string, m)?)?;
    Ok(())
}
