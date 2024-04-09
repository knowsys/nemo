# nemo-cli

This crate contains the cli frontend, i. e. the `nmo` binary. It is the application that will be run by `cargo run`, or if you have nix installed, you might also use `nix run`.

## Usage

```
Nemo CLI

Usage: nmo [OPTIONS] <RULES>...

Arguments:
  <RULES>...  One or more rule program files

Options:
  -e, --export <EXPORT_SETTING>        Override export directives in the program [default: keep] [possible values: keep, none, idb, edb, all]
  -D, --export-dir <EXPORT_DIRECTORY>  Base directory for exporting files [default: results]
  -o, --overwrite-results              Replace any existing files during export
  -g, --gzip                           Use gzip to compress exports by default; does not affect export directives that already specify a compression
  -I, --import-dir <IMPORT_DIRECTORY>  Base directory for importing files (default is working directory)
      --trace <TRACED_FACTS>           Facts for which a derivation trace should be computed; multiple facts can be separated by a semicolon
      --trace-output <OUTPUT_FILE>     File to export the trace to
      --report <REPORTING>             Control amount of reporting printed by the program [default: auto] [possible values: none, auto, short, time, mem, all]
  -v, --verbose...                     Increase log verbosity (multiple uses increase verbosity further)
  -q, --quiet                          Reduce log verbosity to show only errors (equivalent to --log error)
      --log <LOG_LEVEL>                Set log verbosity (default is "warn") [possible values: error, warn, info, debug, trace]
  -h, --help                           Print help (see more with '--help')
  -V, --version                        Print version
```

## Log-Levels

The log-level is set by the following parameters (in order of decreasing precedence):

* `info`, `debug`, `trace`; depending on the count of `-v`
* `error` when `-q` is used
* value of `--log`
* value of the `NMO_LOG` environment variable
* `warn` otherwise
