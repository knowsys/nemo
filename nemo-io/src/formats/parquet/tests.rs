//! Tests of reading and writing Parquet files.

use std::fs::File;

use arrow::{array::RecordBatch, datatypes::DataType};
use parquet::arrow::ArrowWriter;

use nemo_physical::{dictionary::Dictionary, function::EvaluationContext, value::DataValue};

use crate::{selection::RowSelection, testing::TempDirectory};

use super::{ParquetReader, ParquetWriter};

/// A written file is read back:
/// columns of one kind keep their values, with floats and doubles apart,
/// and a column of mixed kinds goes through text.
#[test]
fn written_parquet_reads_back() {
    let rows: Vec<Vec<DataValue>> = vec![
        vec![
            DataValue::integer(1),
            DataValue::double(0.5),
            DataValue::string("plain"),
            DataValue::float(0.25),
        ],
        vec![
            DataValue::integer(2),
            DataValue::double(1.5),
            DataValue::iri("http://example.org/a"),
            DataValue::float(1.25),
        ],
    ];
    let directory = TempDirectory::new();
    let path = directory.path().join("rows.parquet");
    let written = ParquetWriter::new(4)
        .write_path(&path, rows.iter().map(Vec::as_slice))
        .expect("the rows write");
    assert_eq!(written.rows, 2);

    assert_eq!(ParquetReader::width(&path).expect("the schema reads"), 4);
    let mut dictionary = Dictionary::new();
    let loaded = ParquetReader::new()
        .read_path(&path, &mut dictionary, EvaluationContext::now())
        .expect("the file reads back");
    assert_eq!((loaded.rows.len(), loaded.counts.malformed), (2, 0));
    let read = loaded.decoded(&dictionary);
    assert_eq!(read[0][0], DataValue::integer(1));
    assert_eq!(read[1][1], DataValue::double(1.5));
    assert_eq!(read[0][2], DataValue::string("plain"));
    assert_eq!(read[0][3], DataValue::float(0.25));

    // The mixed third column went through text, so the IRI is read back
    // as a string of its text.
    assert_eq!(read[1][2], DataValue::string("http://example.org/a"));
}

/// A null in a column that is not delivered still makes its row malformed.
#[test]
fn a_column_left_out_still_decides_whether_a_row_is_well_formed() {
    use std::sync::Arc;

    use arrow::{
        array::Int64Array,
        datatypes::{Field, Schema},
    };

    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Int64Array::from(vec![Some(10), None])),
        ],
    )
    .expect("the batch builds");
    let directory = TempDirectory::new();
    let path = directory.path().join("nulls.parquet");
    let mut writer =
        ArrowWriter::try_new(File::create(&path).expect("the file opens"), schema, None)
            .expect("the writer starts");
    writer.write(&batch).expect("the batch writes");
    writer.close().expect("the file closes");

    let selection = RowSelection {
        columns: Some(vec![true, false]),
        condition: None,
    };
    let loaded = ParquetReader::with_selection(selection)
        .read_path(&path, &mut Dictionary::new(), EvaluationContext::now())
        .expect("the file reads");

    // The second row's null is a missing value, whether its column is delivered or not.
    assert_eq!(
        (
            loaded.rows.len(),
            loaded.counts.missing,
            loaded.counts.malformed
        ),
        (1, 1, 0)
    );
    assert_eq!(loaded.rows.arity(), 1);
}

/// A file written without rows keeps the writer's number of columns,
/// and its load has as many columns as the reader delivers.
#[test]
fn a_file_without_rows_keeps_its_columns() {
    let directory = TempDirectory::new();
    let path = directory.path().join("empty.parquet");
    let written = ParquetWriter::new(2)
        .write_path(&path, std::iter::empty())
        .expect("no rows write");
    assert_eq!(written.rows, 0);
    assert_eq!(ParquetReader::width(&path).expect("the schema reads"), 2);

    let read = |reader: ParquetReader| {
        reader
            .read_path(&path, &mut Dictionary::new(), EvaluationContext::now())
            .expect("the file reads")
    };
    let whole = read(ParquetReader::new());
    assert_eq!((whole.rows.len(), whole.rows.arity()), (0, 2));

    let selection = RowSelection {
        columns: Some(vec![false, true]),
        condition: None,
    };
    let selected = read(ParquetReader::with_selection(selection));
    assert_eq!((selected.rows.len(), selected.rows.arity()), (0, 1));
}
