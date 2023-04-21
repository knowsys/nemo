/*!
  Binary for the CLI of nemo: nmo
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
use colored::Colorize;

fn main() {
    let mut app = app::CliApp::parse();
    if let Err(err) = app.run() {
        eprintln!("{} {err}", "Application Error:".red().bold());
        std::process::exit(1);
    }
}
