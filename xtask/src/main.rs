mod bin_name;
mod commands;

use clap::Parser;

use commands::{Task, XtaskCli};

fn main() -> std::io::Result<()> {
    let args = XtaskCli::parse();

    args.handle()
}
