/*!
  Binary for the CLI of stage2
*/

#![deny(
    missing_debug_implementations,
    missing_copy_implementations,
    trivial_casts,
    trivial_numeric_casts
)]
#![warn(
    missing_docs,
    unused_import_braces,
    unused_qualifications,
    unused_extern_crates,
    variant_size_differences
)]
#![feature(macro_metavar_expr)]
#![feature(is_sorted)]

pub mod app;

use clap::Parser;

use stage2::meta::timing::TimedDisplay;
use stage2::meta::TimedCode;

fn main() {
    TimedCode::instance().start();
    let mut app = app::CliApp::parse();
    if let Err(err) = app.run() {
        eprintln!("Application Error: {err}");
        std::process::exit(1);
    }
    TimedCode::instance().stop();
    println!(
        "\n{}",
        TimedCode::instance().create_tree_string(
            "stage2",
            &[
                TimedDisplay::default(),
                TimedDisplay::default(),
                TimedDisplay::new(stage2::meta::timing::TimedSorting::LongestThreadTime, 0)
            ]
        )
    );
}
