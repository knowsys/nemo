//! Buildscript to automatically generate test-cases
//! Idea found at https://blog.cyplo.dev/posts/2018/12/generate-rust-tests-from-data
//! Taken and adapted from adf-obdd project
use std::env;
use std::fs::read_dir;
use std::fs::read_to_string;
use std::fs::DirEntry;
use std::fs::File;
use std::io::Write;
use std::path::Path;

// build script's entry point
fn main() {
    gen_tests();
}

fn gen_tests() {
    let out_dir = env::var("OUT_DIR").unwrap();
    let destination = Path::new(&out_dir).join("tests.rs");
    let mut test_file = File::create(destination).unwrap();

    let manifest = env!("CARGO_MANIFEST_DIR");
    if let Ok(test_data_directory) = read_dir(&format!("{manifest}/tests/cases")) {
        // write test file header, put `use`, `const` etc there
        write_header(manifest, &mut test_file);

        for file in test_data_directory {
            write_test(&mut test_file, &file.unwrap());
        }
    }
}

fn write_test(test_file: &mut File, file: &DirEntry) {
    let file_path = file.path();
    let name = file_path.file_name().unwrap();

    write!(
        test_file,
        include_str!("./tests/test_template"),
        name = name.to_str().unwrap(),
        path = file_path.as_os_str().to_str().unwrap(),
    )
    .unwrap();
}

fn write_header(manifest: &str, test_file: &mut File) {
    let execution = read_to_string(format!("{manifest}/tests/rule_execution.rs")).unwrap();

    write!(test_file, "{execution}").unwrap();
}
