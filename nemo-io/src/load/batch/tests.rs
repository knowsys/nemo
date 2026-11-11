//! Tests that batches agree with sequential reads: windows, bytes in memory, line
//! numbers, column formats, conditions, and limits.

use super::*;

use std::{collections::HashSet, fs, io::Cursor};

use nemo_physical::{
    storage::StorageValue,
    value::{DataValue, EvaluationError},
};

use crate::{
    AboutResource,
    dsv::DsvReaderOptions,
    load::Reader,
    testing::{TempDirectory, greater_than},
    value_format::ValueFormat,
};

/// Three files loaded as a batch are interned as the same values that sequential reads give,
/// in the order of the files.
#[test]
fn batches_match_sequential_reads() {
    let temporary = TempDirectory::new();
    let directory = temporary.path();
    let contents = ["x,1\nshared,2\n", "shared,3\n", "y,4\nx,5\n"];
    let mut paths = Vec::new();
    for (index, content) in contents.iter().enumerate() {
        let path = directory.join(format!("nemo_io_batch_{index}.csv"));
        fs::write(&path, content).expect("the temp file writes");
        paths.push(path);
    }

    let sources: Vec<DsvResource> = paths
        .iter()
        .map(|path| DsvResource::path(path.clone(), DsvReaderOptions::csv_terms(2)))
        .collect();

    let mut batch_dictionary = Dictionary::new();
    let batch = ResourceBatch::new(sources.clone());
    let batch_loaded = batch
        .load(&mut batch_dictionary, EvaluationContext::now())
        .expect("the batch loads");

    let mut sequential_dictionary = Dictionary::new();
    let sequential: Vec<Loaded> = paths
        .iter()
        .map(|path| {
            DsvReader::new(DsvReaderOptions::csv_terms(2))
                .read_path(path, &mut sequential_dictionary, EvaluationContext::now())
                .expect("the source reads")
        })
        .collect();

    assert_eq!(batch_loaded.len(), sequential.len());
    for (batch_rows, sequential_rows) in batch_loaded.iter().zip(&sequential) {
        assert_eq!(
            batch_rows.decoded(&batch_dictionary),
            sequential_rows.decoded(&sequential_dictionary)
        );
    }
}

/// Bytes in memory load as a streaming read of the same content does,
/// also when they are cut into windows and parsed in parallel.
#[test]
fn byte_sources_match_file_reads() {
    let content: String = (0..500)
        .map(|index| format!("name-{index}\t{index}\n"))
        .collect();
    let mut options = DsvReaderOptions::tsv(vec![ValueFormat::Term, ValueFormat::Term]);
    options.quoting = false;

    let mut batch = ResourceBatch::new(vec![DsvResource::bytes(
        "held.tsv",
        content.clone().into_bytes(),
        options.clone(),
    )]);
    batch.window_bytes = 256;
    assert!(batch.windows(0).len() > 2, "the buffer split");
    let mut bytes_dictionary = Dictionary::new();
    let from_bytes = batch
        .load(&mut bytes_dictionary, EvaluationContext::now())
        .expect("the batch loads");

    let mut read_dictionary = Dictionary::new();
    let from_read = DsvReader::new(options)
        .read(
            Cursor::new(content),
            &mut read_dictionary,
            EvaluationContext::now(),
        )
        .expect("the source reads");

    assert_eq!(from_bytes[0].rows.len(), 500);
    assert_eq!(
        from_bytes[0].decoded(&bytes_dictionary),
        from_read.decoded(&read_dictionary)
    );
}

/// A batch keeps the row count of a resource whose columns are all skipped,
/// as a streaming read does.
#[test]
fn all_skip_sources_keep_their_record_count_in_batches() {
    // A resource of arity zero has its rows only as a count.
    let temporary = TempDirectory::new();
    let directory = temporary.path();
    let path = directory.join("nemo_io_batch_skip.csv");
    fs::write(&path, "a,1\nb,2\n").expect("the temp file writes");
    let options = DsvReaderOptions::csv(vec![ValueFormat::Skip, ValueFormat::Skip]);
    let source = DsvResource::path(path.clone(), options.clone());

    let mut dictionary = Dictionary::new();
    let batched = ResourceBatch::new(vec![source.clone(), source.clone()])
        .load(&mut dictionary, EvaluationContext::now())
        .expect("the batch loads");
    let streamed = DsvReader::new(options)
        .read_path(&path, &mut dictionary, EvaluationContext::now())
        .expect("the source reads");

    assert_eq!(streamed.rows.len(), 2);
    assert_eq!(batched[0].rows.len(), 2);
    assert_eq!(batched[1].rows.len(), 2);
}

/// A batch loads a wide value as a streaming read does.
#[test]
fn batches_and_streams_agree_on_wide_values() {
    // Line 2's dateTime lies beyond the compact instant range, so it is wide.
    let temporary = TempDirectory::new();
    let directory = temporary.path();
    let path = directory.join("nemo_io_batch_wide_value.csv");
    let content = "a\n\"999999-01-01T00:00:00\"^^<http://www.w3.org/2001/XMLSchema#dateTime>\nb\n";
    fs::write(&path, content).expect("the temp file writes");
    let mut options = DsvReaderOptions::csv(vec![ValueFormat::Term]);
    options.quoting = false;
    let source = DsvResource::path(path.clone(), options.clone());

    let mut dictionary = Dictionary::new();
    let batched = ResourceBatch::new(vec![source.clone(), source.clone()])
        .load(&mut dictionary, EvaluationContext::now())
        .expect("the batch loads");
    let streamed = DsvReader::new(options)
        .read_path(&path, &mut dictionary, EvaluationContext::now())
        .expect("the source reads");

    assert_eq!(streamed.counts.malformed, 0);
    assert_eq!(batched[0].counts.malformed, 0);
    assert_eq!(streamed.rows.len(), 3);
    assert_eq!(batched[0].rows.len(), streamed.rows.len());
}

/// A file cut into many windows, several per worker,
/// loads the same rows in the same order as a sequential read.
#[test]
fn windowed_batches_match_sequential_reads() {
    let temporary = TempDirectory::new();
    let directory = temporary.path();
    let path = directory.join("nemo_io_windowed.tsv");
    let content: String = (0..500)
        .map(|index| format!("name-{index}\t{index}\n"))
        .collect();
    fs::write(&path, &content).expect("the temp file writes");
    let mut options = DsvReaderOptions::tsv(vec![ValueFormat::Term, ValueFormat::Term]);
    options.quoting = false;
    let source = DsvResource::path(path.clone(), options.clone());

    let mut batch = ResourceBatch::new(vec![source.clone()]);
    batch.window_bytes = 64;
    let mut windowed_dictionary = Dictionary::new();
    let windowed = batch
        .load(&mut windowed_dictionary, EvaluationContext::now())
        .expect("the batch loads");

    let mut sequential_dictionary = Dictionary::new();
    let sequential = DsvReader::new(options)
        .read_path(&path, &mut sequential_dictionary, EvaluationContext::now())
        .expect("the source reads");

    assert!(
        batch.windows(0).len() > 50,
        "the file split into many windows"
    );
    assert_eq!(windowed[0].rows.len(), 500);
    assert_eq!(
        windowed[0].decoded(&windowed_dictionary),
        sequential.decoded(&sequential_dictionary)
    );
}

/// A label names one null across the windows of a file,
/// and the rows with nulls keep their places among the other rows.
#[test]
fn a_label_names_one_null_across_the_windows_of_a_file() {
    let temporary = TempDirectory::new();
    let path = temporary.path().join("nulls.tsv");
    let content: String = (0..300)
        .map(|index| match index % 3 {
            0 => format!("_:shared\t{index}\n"),
            1 => format!("_:own-{index}\t{index}\n"),
            _ => format!("name-{index}\t{index}\n"),
        })
        .collect();
    fs::write(&path, &content).expect("the temp file writes");
    let mut options = DsvReaderOptions::tsv(vec![ValueFormat::Term, ValueFormat::Term]);
    options.quoting = false;

    let mut batch = ResourceBatch::new(vec![DsvResource::path(path, options)]);
    batch.window_bytes = 64;
    let mut dictionary = Dictionary::new();
    let loaded = batch
        .load(&mut dictionary, EvaluationContext::now())
        .expect("the batch loads");
    assert!(batch.windows(0).len() > 20, "the file split into windows");

    let rows = loaded[0].decoded(&dictionary);
    assert_eq!(rows.len(), 300);
    let mut own = HashSet::new();
    for (index, row) in (0i64..).zip(&rows) {
        assert_eq!(row[1], DataValue::integer(index));
        match index % 3 {
            0 => assert_eq!(row[0], rows[0][0], "row {index}"),
            1 => assert!(
                own.insert(row[0].clone()) && row[0] != rows[0][0],
                "row {index}"
            ),
            _ => assert_eq!(row[0], DataValue::iri(format!("name-{index}"))),
        }
    }
}

/// The header of a file with named columns is read once,
/// and every window of the file finds its columns by it.
#[test]
fn the_windows_of_a_file_share_its_header() {
    let temporary = TempDirectory::new();
    let path = temporary.path().join("named.tsv");
    let content: String = std::iter::once(String::from(
        "number	unread	name
",
    ))
    .chain((0..300).map(|index| {
        format!(
            "{index}	not a term	name-{index}
"
        )
    }))
    .collect();
    fs::write(&path, &content).expect("the temp file writes");
    let mut options = DsvReaderOptions::tsv([ValueFormat::Term, ValueFormat::Integer]);
    options.quoting = false;
    options.columns = Some(vec![String::from("name"), String::from("number")]);

    let mut batch = ResourceBatch::new(vec![DsvResource::path(path.clone(), options.clone())]);
    batch.window_bytes = 64;
    let mut dictionary = Dictionary::new();
    let loaded = batch
        .load(&mut dictionary, EvaluationContext::now())
        .expect("the batch loads");
    assert!(batch.windows(0).len() > 20, "the file split into windows");

    let rows = loaded[0].decoded(&dictionary);
    assert_eq!((rows.len(), loaded[0].counts.malformed), (300, 0));
    for (index, row) in (0i64..).zip(&rows) {
        let name = DataValue::iri(format!("name-{index}"));
        assert_eq!(row[..], [name, DataValue::integer(index)]);
    }

    // A header without one of the names fails the batch, and the error names the file.
    options.columns = Some(vec![String::from("name"), String::from("missing")]);
    let problem = ResourceBatch::new(vec![DsvResource::path(path.clone(), options)])
        .load(&mut dictionary, EvaluationContext::now())
        .expect_err("the header has no such column");
    assert_eq!(problem.code(), crate::codes::HEADER_LACKS_COLUMN);
    assert_eq!(
        problem.resource(),
        Some(path.display().to_string().as_str())
    );
}

/// A malformed row deep in a later window is reported at its line in the file,
/// not in its window.
#[test]
fn windowed_malformed_rows_keep_their_global_lines() {
    let temporary = TempDirectory::new();
    let directory = temporary.path();
    let path = directory.join("nemo_io_windowed_malformed.tsv");
    let mut content: String = (0..400)
        .map(|index| format!("name-{index}\t{index}\n"))
        .collect();
    content.push_str("broken\n");
    content.push_str("name-tail\t400\n");
    fs::write(&path, &content).expect("the temp file writes");
    let mut options = DsvReaderOptions::tsv(vec![ValueFormat::Term, ValueFormat::Integer]);
    options.quoting = false;
    let source = DsvResource::path(path.clone(), options);

    let mut batch = ResourceBatch::new(vec![source]);
    batch.window_bytes = 256;
    let mut dictionary = Dictionary::new();
    let loaded = batch
        .load(&mut dictionary, EvaluationContext::now())
        .expect("the batch loads");

    assert_eq!(loaded[0].rows.len(), 401);
    assert_eq!(loaded[0].counts.malformed, 1);
    assert_eq!(loaded[0].counts.first_malformed, Some(401));
}

/// A batch counts malformed and excluded rows as a streaming read does.
#[test]
fn rows_are_counted_alike_in_both_paths() {
    // Column 1 is read as `int`, and the condition keeps rows with column 1
    // above 5. Line 2 is not an integer and line 3 is short,
    // so both are malformed. Line 4 fails the condition.
    let temporary = TempDirectory::new();
    let directory = temporary.path();
    let path = directory.join("nemo_io_counted_alike.csv");
    fs::write(
        &path,
        "a,7
b,x
c
d,3
e,9
",
    )
    .expect("the temp file writes");
    let mut options = DsvReaderOptions::csv(vec![ValueFormat::Term, ValueFormat::Integer]);
    options.condition = Some(greater_than(1, 5));

    let mut dictionary = Dictionary::new();
    let streamed = DsvReader::new(options.clone())
        .read_path(&path, &mut dictionary, EvaluationContext::now())
        .expect("the source reads");
    let batched = ResourceBatch::new(vec![DsvResource::path(path.clone(), options)])
        .load(&mut dictionary, EvaluationContext::now())
        .expect("the batch loads");
    for loaded in [&streamed, &batched[0]] {
        assert_eq!(loaded.rows.len(), 2);
        assert_eq!(loaded.counts.malformed, 2);
        assert_eq!(loaded.counts.excluded, 1);
    }
}

/// A row on which the condition fails with an error is dropped and counted under the error's
/// kind, not as excluded, in a batch as in a streaming read.
#[test]
fn an_erroring_condition_is_counted_by_its_kind_in_both_paths() {
    // Line 2's column 1 is a string the condition cannot compare with a number.
    let temporary = TempDirectory::new();
    let directory = temporary.path();
    let path = directory.join("nemo_io_filter_errors.csv");
    fs::write(&path, "a,7\nb,x\nc,3\n").expect("the temp file writes");
    let mut options = DsvReaderOptions::csv(vec![ValueFormat::Term, ValueFormat::Term]);
    options.condition = Some(greater_than(1, 5));

    let mut dictionary = Dictionary::new();
    let streamed = DsvReader::new(options.clone())
        .read_path(&path, &mut dictionary, EvaluationContext::now())
        .expect("the source reads");
    let batched = ResourceBatch::new(vec![DsvResource::path(path.clone(), options)])
        .load(&mut dictionary, EvaluationContext::now())
        .expect("the batch loads");
    for loaded in [&streamed, &batched[0]] {
        assert_eq!(loaded.rows.len(), 1);
        assert_eq!(loaded.counts.excluded, 1);
        let errors = loaded.counts.errors;
        assert_eq!(
            (errors.count(EvaluationError::Incomparable), errors.total()),
            (1, 1)
        );
    }
}

/// A resource with a limit stays one window, so its rows and counts match a sequential read,
/// whatever the number of cores.
#[test]
fn limited_sources_read_as_one_window_and_count_like_the_streaming_reader() {
    // A limit counts rows in the resource's order, which windows parsed separately cannot do.
    // The condition drops rows, and there are malformed lines among the first rows.
    let temporary = TempDirectory::new();
    let directory = temporary.path();
    let path = directory.join("nemo_io_limited.tsv");
    let mut content = String::new();
    for index in 0..500 {
        if index % 100 == 50 {
            content.push_str("broken\n");
        }

        content.push_str(&format!("name-{index}\t{index}\n"));
    }

    fs::write(&path, &content).expect("the temp file writes");
    let mut options = DsvReaderOptions::tsv(vec![ValueFormat::Term, ValueFormat::Integer]);
    options.quoting = false;
    options.condition = Some(greater_than(1, 5));
    options.limit = Some(200);
    let source = DsvResource::path(path.clone(), options.clone());

    let mut batch = ResourceBatch::new(vec![source]);
    batch.window_bytes = 256;
    assert_eq!(batch.windows(0).len(), 1, "a limited source is one window");
    let mut windowed_dictionary = Dictionary::new();
    let windowed = batch
        .load(&mut windowed_dictionary, EvaluationContext::now())
        .expect("the batch loads");

    let mut sequential_dictionary = Dictionary::new();
    let sequential = DsvReader::new(options)
        .read_path(&path, &mut sequential_dictionary, EvaluationContext::now())
        .expect("the source reads");

    // The first 200 well-formed rows are 0 to 199, past two malformed lines,
    // and the condition drops the six of them that are not above 5.
    assert_eq!(sequential.rows.len(), 194);
    assert_eq!(
        (sequential.counts.excluded, sequential.counts.malformed),
        (6, 2)
    );
    assert_eq!(windowed[0].rows.len(), sequential.rows.len());
    assert_eq!(windowed[0].counts.excluded, sequential.counts.excluded);
    assert_eq!(windowed[0].counts.malformed, sequential.counts.malformed);
    assert_eq!(
        windowed[0].counts.first_malformed,
        sequential.counts.first_malformed
    );
    assert_eq!(
        windowed[0].decoded(&windowed_dictionary),
        sequential.decoded(&sequential_dictionary)
    );
}

/// Wherever the windows and runs fall, a batch interns the same values as a sequential read,
/// in the same order, so even the raw storage values agree, and it reports the same lines.
#[test]
fn runs_intern_like_a_straight_read() {
    use std::io::Write;

    use flate2::write::GzEncoder;

    // Enough rows for several runs per window and enough bytes for
    // the pool, with malformed rows in the first and in a later run.
    let temporary = TempDirectory::new();
    let directory = temporary.path();
    let mut content = String::new();
    for index in 0..60_000 {
        if index == 2 || index == 45_000 {
            content.push_str("broken\n");
        }

        content.push_str(&format!(
            "name-{}\t{index}\t\"text {}\"\n",
            index % 1000,
            index % 777
        ));
    }

    let plain = directory.join("nemo_io_runs.tsv");
    fs::write(&plain, &content).expect("the temp file writes");
    let compressed = directory.join("nemo_io_runs.tsv.gz");
    let mut encoder = GzEncoder::new(
        File::create(&compressed).expect("the temp file creates"),
        flate2::Compression::fast(),
    );
    encoder
        .write_all(content.as_bytes())
        .expect("the temp file writes");
    encoder.finish().expect("the temp file writes");

    let mut options = DsvReaderOptions::tsv(vec![
        ValueFormat::Iri,
        ValueFormat::Integer,
        ValueFormat::Term,
    ]);
    options.quoting = false;
    let mut filtered = options.clone();
    filtered.condition = Some(greater_than(1, 5));

    for options in [options, filtered] {
        for (path, window_bytes) in [
            (&plain, WINDOW_BYTES),
            (&plain, 64 * 1024),
            (&compressed, WINDOW_BYTES),
        ] {
            let mut batch =
                ResourceBatch::new(vec![DsvResource::path(path.clone(), options.clone())]);
            batch.window_bytes = window_bytes;
            let mut dictionary = Dictionary::new();
            let batched = batch
                .load(&mut dictionary, EvaluationContext::now())
                .expect("the batch loads");

            let mut sequential_dictionary = Dictionary::new();
            let sequential = DsvReader::new(options.clone())
                .read_path(path, &mut sequential_dictionary, EvaluationContext::now())
                .expect("the source reads");

            let raw = |loaded: &Loaded| -> Vec<Vec<StorageValue>> {
                (0..loaded.rows.len())
                    .map(|index| loaded.rows.row(index).to_vec())
                    .collect()
            };
            let batched = &batched[0];
            assert_eq!(raw(batched), raw(&sequential), "{path:?} {window_bytes}");
            assert_eq!(batched.counts.malformed, 2);
            assert_eq!(batched.counts.first_malformed, Some(3));
            assert_eq!(
                (batched.counts.well_formed, batched.counts.excluded),
                (sequential.counts.well_formed, sequential.counts.excluded)
            );
        }
    }
}

/// A missing file fails a read and a batch.
#[test]
fn missing_files_are_reported() {
    let temporary = TempDirectory::new();
    let path = temporary.path().join("does_not_exist.csv");
    let mut dictionary = Dictionary::new();
    assert!(
        DsvReader::new(DsvReaderOptions::csv_terms(1))
            .read_path(&path, &mut dictionary, EvaluationContext::now())
            .is_err()
    );
    assert!(
        ResourceBatch::new(vec![DsvResource::path(
            path,
            DsvReaderOptions::csv_terms(1)
        )])
        .load(&mut dictionary, EvaluationContext::now())
        .is_err()
    );
}
