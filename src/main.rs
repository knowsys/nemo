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

use colored::*;
use stage2::meta::TimedCode;

fn main() {
    TimedCode::instance().start();
    let mut app = app::CliApp::parse();
    let new_facts = match app.run() {
        Ok(num_facts) => num_facts,
        Err(err) => {
            eprintln!("Application Error: {err}");
            std::process::exit(1);
        }
    };

    TimedCode::instance().stop();

    let reasoning_time = TimedCode::instance()
        .sub("Reasoning")
        .total_system_time()
        .as_millis();
    let loading_time = TimedCode::instance()
        .sub("Reasoning/Execution/Load Table")
        .total_system_time()
        .as_millis();

    let output_time = reasoning_time - loading_time;

    println!("");
    println!(
        "Finished Reasoning in {}{}. Derived {} facts.",
        output_time.to_string().green().bold(),
        "ms".green().bold(),
        new_facts.to_string().green().bold()
    )

    // println!(
    //     "\n{}",
    //     TimedCode::instance().create_tree_string(
    //         "stage2",
    //         &[
    //             TimedDisplay::default(),
    //             TimedDisplay::default(),
    //             TimedDisplay::new(stage2::meta::timing::TimedSorting::LongestThreadTime, 0)
    //         ]
    //     )
    // );
}
