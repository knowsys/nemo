# nemo-cli

This crate contains the cli frontend, i. e. the `nmo` binary. It is the application that will be run by `cargo run`, or if you have nix installed, you might also use `nix run`.

## Usage

```
Nemo CLI

Usage: nmo [OPTIONS] <RULES>...

Arguments:
  <RULES>...  One or more rule program files

Options:
      --log <LOG_LEVEL>                Sets the verbosity of logging if the flags -v and -q are not used [possible values: error, warn, info, debug, trace]
  -v, --verbose...                     Sets log verbosity (multiple times means more verbose)
  -q, --quiet                          Sets log verbosity to only log errors
  -s, --save-results                   Save results to files. (Also see --output-dir)
  -D, --output-dir <OUTPUT_DIRECTORY>  Specify directory for output files. (Only relevant if --save-results is set.) [default: results]
  -o, --overwrite-results              Overwrite existing files in --output-dir. (Only relevant if --save-results is set.)
  -g, --gzip                           Gzip output files
      --write-all-idb-predicates       Override @output directives and save every IDB predicate
      --detailed-timing                Display detailed timing information
  -h, --help                           Print help
  -V, --version                        Print version
```