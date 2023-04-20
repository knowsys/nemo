# Nemo

[![dependency status](https://deps.rs/repo/github/knowsys/nemo/status.svg)](https://deps.rs/repo/github/knowsys/nemo)

*Nemo* is a datalog-based rule engine for fast and scalable analytic data processing in memory. It is available as a command-line tool ```nmo```.

Goals of Nemo are performance, declarativity, versatility, and reliability. It is written in Rust. Nemo's data model aims at compatibility with [RDF](https://www.w3.org/TR/rdf11-concepts/)/[SPARQL](https://www.w3.org/TR/sparql11-overview/) while preservig established logic programming conventions and features. The following formats are currently supported:
- Input: CSV, TSV
- Rules: datalog dialect with support for existential rules (tuple-generating dependencies) and datatypes
- Output: CSV

Nemo's datatypes allow the use of RDF-style data values but also "plain" names and constants in any of these formats.

Nemo is in heavy development and the current releases should still be considered unstable. 

## Installation

To build from source, download the source code (from a release or this repository) and run

 `cargo build -r`
 
This will create the command-line client `nmo` in the directory `./target/release/`.

## Usage

Run the following command for an overview of current options:

`nmo --help`

## Help

Feel free to use [GitHub discussions](https://github.com/knowsys/nemo/discussions) to ask questions or talk about Nemo.

[Bug reports](https://github.com/knowsys/nemo/issues) are also very welcome.

## License

This project is licensed under either of

- Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or
  https://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or
  https://opensource.org/licenses/MIT)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in Nemo by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.

## Development

We use the following commands before committing:
- `cargo fmt` for our default formatting 
- `cargo clippy --all-targets`, followed by a (manual) repair of any issues

## Acknowledgements

Nemo is developed by the [Knowledge-Based Systems](https://kbs.inf.tu-dresden.de/) group at [TU Dresden](https://tu-dresden.de). Github provides the [list of code contributors](https://github.com/knowsys/nemo/graphs/contributors).

Special thanks are due to [VLog](https://github.com/karmaresearch/vlog), the conceptual predecessor of Nemo and a source of some of the tricks we use.
