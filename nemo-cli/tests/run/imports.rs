//! Imports: the standard streams as the empty resource, remote resources, and the directories
//! relative imports resolve against.

use std::fs;

use crate::common::{run, run_with_stdin, workspace, write};

/// An HTTP server on a loopback port responding to every request for
/// `/data.csv` with a body, and 404 otherwise.
fn serve_csv(body: &'static str) -> String {
    use std::io::{Read, Write};

    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("a loopback port");
    let address = listener.local_addr().expect("an address");
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let mut request = Vec::new();
            let mut buffer = [0u8; 1024];
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                match stream.read(&mut buffer) {
                    Ok(0) | Err(_) => break,
                    Ok(count) => request.extend_from_slice(&buffer[..count]),
                }
            }
            let found = String::from_utf8_lossy(&request).starts_with("GET /data.csv ");
            let (status, body) = if found {
                ("200 OK", body)
            } else {
                ("404 Not Found", "gone")
            };
            let response = format!(
                "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).ok();
        }
    });
    format!("http://{address}")
}

#[test]
fn the_empty_resource_is_the_standard_streams() {
    let directory = workspace("pipes");
    write(
        &directory,
        "program.rls",
        "@import p from csv { resource = \"\" } .\n\
         q(?x) :- p(?x, ?y) .\n\
         @export q to csv { resource = \"\" } .\n",
    );
    let outcome = run_with_stdin(&directory, &["program.rls", "-q"], "1,a\n2,b\n");
    assert_eq!(outcome.status, Some(0), "{}", outcome.stderr);
    let mut rows: Vec<&str> = outcome.stdout.lines().collect();
    rows.sort_unstable();
    assert_eq!(rows, ["1", "2"], "read from stdin, written to stdout");
    assert!(!directory.join("results").exists(), "no file was written");
}

#[test]
fn remote_imports_are_fetched_and_their_failures_reported() {
    let directory = workspace("remote");
    let base = serve_csv("1\n2\n");
    write(
        &directory,
        "program.rls",
        &format!(
            "@import p from csv {{ resource = \"{base}/data.csv\" }} .\n\
             q(?x) :- p(?x) .\n\
             @output q .\n"
        ),
    );
    let outcome = run(&directory, &["program.rls", "--print"]);
    assert_eq!(outcome.status, Some(0), "{}", outcome.stderr);
    let mut facts: Vec<&str> = outcome.stdout.lines().collect();
    facts.sort_unstable();
    assert_eq!(facts, ["q(1) .", "q(2) ."]);

    write(
        &directory,
        "missing.rls",
        &format!(
            "@import p from csv {{ resource = \"{base}/missing.csv\" }} .\n\
             q(?x) :- p(?x) .\n\
             @output q .\n"
        ),
    );
    let outcome = run(&directory, &["missing.rls", "--print"]);
    assert_eq!(outcome.status, Some(1), "{}", outcome.stderr);
    assert!(outcome.stderr.contains("N0111"), "{}", outcome.stderr);

    write(
        &directory,
        "warned.rls",
        &format!(
            "@import p from csv {{ resource = \"{base}/missing.csv\", on_error = warn }} .\n\
             q(?x) :- p(?x) .\n\
             @output q .\n"
        ),
    );
    let outcome = run(&directory, &["warned.rls", "--print"]);
    assert_eq!(outcome.status, Some(3), "{}", outcome.stderr);
    assert!(outcome.stderr.contains("N0333"), "{}", outcome.stderr);
    assert_eq!(outcome.stdout, "");
}

#[test]
fn each_program_file_reads_its_imports_beside_itself() {
    let directory = workspace("imports-beside");
    for name in ["first", "second"] {
        fs::create_dir_all(directory.join(name)).expect("the workspace is writable");
    }
    write(&directory.join("first"), "rows.csv", "1\n");
    write(&directory.join("second"), "rows.csv", "2\n");
    write(
        &directory.join("first"),
        "a.rls",
        "@import p from csv { resource = \"rows.csv\" } .\n@output p .\n",
    );
    write(
        &directory.join("second"),
        "b.rls",
        "@import q from csv { resource = \"rows.csv\" } .\n@output q .\n",
    );

    let outcome = run(&directory, &["first/a.rls", "second/b.rls", "-P"]);
    assert_eq!(outcome.status, Some(0), "{}", outcome.stderr);
    assert!(outcome.stdout.contains("p(1)"), "{}", outcome.stdout);
    assert!(outcome.stdout.contains("q(2)"), "{}", outcome.stdout);
}

#[test]
fn the_import_directory_resolves_every_relative_import() {
    let directory = workspace("import-directory");
    fs::create_dir_all(directory.join("data")).expect("the workspace is writable");
    write(&directory.join("data"), "rows.csv", "3\n");
    write(
        &directory,
        "program.rls",
        "@import p from csv { resource = \"rows.csv\" } .\n@output p .\n",
    );

    let outcome = run(&directory, &["program.rls", "-I", "data", "-P"]);
    assert_eq!(outcome.status, Some(0), "{}", outcome.stderr);
    assert!(outcome.stdout.contains("p(3)"), "{}", outcome.stdout);
}
