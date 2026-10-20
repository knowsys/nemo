//! Batches agree with sequential reads: windows, byte sources, line
//! numbers, declared types, conditions, and limits.

use super::*;

use std::{fs, io::Cursor};

use nemo_physical::{
    storage::StorageValue,
    value::{DataValue, EvaluationError},
};

use crate::{
    format::ValueFormat,
    formats::dsv::DsvOptions,
    load::Reader,
    testing::{TempDirectory, greater_than_five},
};

#[test]
fn batches_match_sequential_reads() {
    // Three files loaded as a batch must intern to the same values a
    // sequential read produces, in source order.
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
        .map(|path| DsvResource::path(path.clone(), DsvOptions::csv_terms(2)))
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
            DsvReader::new(DsvOptions::csv_terms(2))
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

#[test]
fn byte_sources_match_file_reads() {
    // The same content loads identically whether it comes from disk
    // or from memory, windowed and in parallel included.
    let content: String = (0..500)
        .map(|index| format!("name-{index}\t{index}\n"))
        .collect();
    let mut options = DsvOptions::tsv(vec![ValueFormat::Term, ValueFormat::Term]);
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

#[test]
fn all_skip_sources_keep_their_record_count_in_batches() {
    // A nullary source (every column skipped) carries its rows only
    // as a count; the batch path must preserve it like the
    // streaming path does.
    let temporary = TempDirectory::new();
    let directory = temporary.path();
    let path = directory.join("nemo_io_batch_skip.csv");
    fs::write(&path, "a,1\nb,2\n").expect("the temp file writes");
    let options = DsvOptions::csv(vec![ValueFormat::Skip, ValueFormat::Skip]);
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

#[test]
fn batches_and_streams_agree_on_wide_values() {
    // Line 2's dateTime lies beyond the inline instant range and
    // takes the wide store; the batch path must load it like the
    // streaming path does.
    let temporary = TempDirectory::new();
    let directory = temporary.path();
    let path = directory.join("nemo_io_batch_wide_value.csv");
    let content = "a\n\"999999-01-01T00:00:00\"^^<http://www.w3.org/2001/XMLSchema#dateTime>\nb\n";
    fs::write(&path, content).expect("the temp file writes");
    let mut options = DsvOptions::csv(vec![ValueFormat::Term]);
    options.quoting = false;
    let source = DsvResource::path(path.clone(), options.clone());

    let mut dictionary = Dictionary::new();
    let batched = ResourceBatch::new(vec![source.clone(), source.clone()])
        .load(&mut dictionary, EvaluationContext::now())
        .expect("the batch loads");
    let streamed = DsvReader::new(options)
        .read_path(&path, &mut dictionary, EvaluationContext::now())
        .expect("the source reads");

    assert_eq!(streamed.malformed, 0);
    assert_eq!(batched[0].malformed, 0);
    assert_eq!(streamed.rows.len(), 3);
    assert_eq!(batched[0].rows.len(), streamed.rows.len());
}

#[test]
fn windowed_batches_match_sequential_reads() {
    // A file forced into many windows, several per worker,
    // must load the same rows in the same order a straight read produces.
    let temporary = TempDirectory::new();
    let directory = temporary.path();
    let path = directory.join("nemo_io_windowed.tsv");
    let content: String = (0..500)
        .map(|index| format!("name-{index}\t{index}\n"))
        .collect();
    fs::write(&path, &content).expect("the temp file writes");
    let mut options = DsvOptions::tsv(vec![ValueFormat::Term, ValueFormat::Term]);
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

#[test]
fn windowed_malformed_rows_keep_their_global_lines() {
    // A malformed row deep in a later window must be reported at
    // its line in the file, not in its window.
    let temporary = TempDirectory::new();
    let directory = temporary.path();
    let path = directory.join("nemo_io_windowed_malformed.tsv");
    let mut content: String = (0..400)
        .map(|index| format!("name-{index}\t{index}\n"))
        .collect();
    content.push_str("broken\n");
    content.push_str("name-tail\t400\n");
    fs::write(&path, &content).expect("the temp file writes");
    let mut options = DsvOptions::tsv(vec![ValueFormat::Term, ValueFormat::Integer]);
    options.quoting = false;
    let source = DsvResource::path(path.clone(), options);

    let mut batch = ResourceBatch::new(vec![source]);
    batch.window_bytes = 256;
    let mut dictionary = Dictionary::new();
    let loaded = batch
        .load(&mut dictionary, EvaluationContext::now())
        .expect("the batch loads");

    assert_eq!(loaded[0].rows.len(), 401);
    assert_eq!(loaded[0].malformed, 1);
    assert_eq!(loaded[0].first_malformed, Some(401));
}

#[test]
fn rows_are_counted_alike_in_both_paths() {
    use nemo_physical::function::{OpCode, StackProgram};

    use crate::row_filter::RowFilter;

    // Column 1 reads as `int`, and the filter keeps rows with column 1
    // above 5. Line 2 does not read as an integer and line 3 is short,
    // so both are malformed; line 4 fails the filter.
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
    let mut options = DsvOptions::csv(vec![ValueFormat::Term, ValueFormat::Integer]);
    options.filter = Some(RowFilter::new(
        vec![1],
        StackProgram::new(
            vec![OpCode::Input(0), OpCode::Constant(0), OpCode::Greater],
            vec![DataValue::integer(5)],
            1,
        ),
    ));

    let mut dictionary = Dictionary::new();
    let streamed = DsvReader::new(options.clone())
        .read_path(&path, &mut dictionary, EvaluationContext::now())
        .expect("the source reads");
    let batched = ResourceBatch::new(vec![DsvResource::path(path.clone(), options)])
        .load(&mut dictionary, EvaluationContext::now())
        .expect("the batch loads");
    for loaded in [&streamed, &batched[0]] {
        assert_eq!(loaded.rows.len(), 2);
        assert_eq!(loaded.malformed, 2);
        assert_eq!(loaded.excluded, 1);
    }
}

#[test]
fn an_erroring_condition_is_counted_by_its_kind_in_both_paths() {
    // Line 2's column 1 is a string the condition cannot compare
    // with a number: the row is dropped as a rule's filter would
    // drop it, and counted under that error kind, not as excluded.
    let temporary = TempDirectory::new();
    let directory = temporary.path();
    let path = directory.join("nemo_io_filter_errors.csv");
    fs::write(&path, "a,7\nb,x\nc,3\n").expect("the temp file writes");
    let mut options = DsvOptions::csv(vec![ValueFormat::Term, ValueFormat::Term]);
    options.filter = Some(greater_than_five());

    let mut dictionary = Dictionary::new();
    let streamed = DsvReader::new(options.clone())
        .read_path(&path, &mut dictionary, EvaluationContext::now())
        .expect("the source reads");
    let batched = ResourceBatch::new(vec![DsvResource::path(path.clone(), options)])
        .load(&mut dictionary, EvaluationContext::now())
        .expect("the batch loads");
    for loaded in [&streamed, &batched[0]] {
        assert_eq!(loaded.rows.len(), 1);
        assert_eq!(loaded.excluded, 1);
        let errors = loaded.errors;
        assert_eq!(
            (errors.count(EvaluationError::Incomparable), errors.total()),
            (1, 1)
        );
    }
}

#[test]
fn limited_sources_read_as_one_window_and_count_like_the_streaming_reader() {
    // A limit counts rows in source order, which windows parsed
    // apart cannot share. A limited source stays one window, so its
    // rows and its skipped-row counts match a straight read whatever
    // the machine's core count, with the condition dropping rows and
    // a malformed line among the first rows.
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
    let mut options = DsvOptions::tsv(vec![ValueFormat::Term, ValueFormat::Integer]);
    options.quoting = false;
    options.filter = Some(greater_than_five());
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

    // The first 200 well-formed rows are `0..200` (the six below the
    // condition's bound dropped), past two malformed lines.
    assert_eq!(sequential.rows.len(), 194);
    assert_eq!((sequential.excluded, sequential.malformed), (6, 2));
    assert_eq!(windowed[0].rows.len(), sequential.rows.len());
    assert_eq!(windowed[0].excluded, sequential.excluded);
    assert_eq!(windowed[0].malformed, sequential.malformed);
    assert_eq!(windowed[0].first_malformed, sequential.first_malformed);
    assert_eq!(
        windowed[0].decoded(&windowed_dictionary),
        sequential.decoded(&sequential_dictionary)
    );
}

#[test]
fn runs_intern_like_a_straight_read() {
    use std::io::Write;

    use flate2::write::GzEncoder;

    // Enough rows for several runs per window and enough bytes for
    // the pool, with malformed rows in the first and in a later run:
    // wherever the windows and runs fall, the batch must intern the
    // values a straight read interns, in the same order, so that even
    // the raw storage values agree, and report the same lines.
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

    let mut options = DsvOptions::tsv(vec![
        ValueFormat::Iri,
        ValueFormat::Integer,
        ValueFormat::Term,
    ]);
    options.quoting = false;
    let mut filtered = options.clone();
    filtered.filter = Some(greater_than_five());

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
            assert_eq!(batched.malformed, 2);
            assert_eq!(batched.first_malformed, Some(3));
            assert_eq!(
                (batched.well_formed, batched.excluded),
                (sequential.well_formed, sequential.excluded)
            );
        }
    }
}

#[test]
fn missing_files_are_reported() {
    let temporary = TempDirectory::new();
    let path = temporary.path().join("does_not_exist.csv");
    let mut dictionary = Dictionary::new();
    assert!(
        DsvReader::new(DsvOptions::csv_terms(1))
            .read_path(&path, &mut dictionary, EvaluationContext::now())
            .is_err()
    );
    assert!(
        ResourceBatch::new(vec![DsvResource::path(path, DsvOptions::csv_terms(1))])
            .load(&mut dictionary, EvaluationContext::now())
            .is_err()
    );
}
