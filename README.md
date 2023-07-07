# Nemo

[![Build and Test main](https://img.shields.io/github/actions/workflow/status/knowsys/nemo/build.yml?branch=main&label=build)](https://github.com/knowsys/nemo/actions/workflows/build.yml)
[![dependency status](https://deps.rs/repo/github/knowsys/nemo/status.svg)](https://deps.rs/repo/github/knowsys/nemo)
[![built with nix](https://img.shields.io/static/v1?logo=nixos&logoColor=white&label=&message=Built%20with%20Nix&color=41439a)](https://builtwithnix.org)

*Nemo* is a datalog-based rule engine for fast and scalable analytic data processing in memory. It is available as a command-line tool ```nmo```.

Goals of Nemo are performance, declarativity, versatility, and reliability. It is written in Rust. Nemo's data model aims at compatibility with [RDF](https://www.w3.org/TR/rdf11-concepts/)/[SPARQL](https://www.w3.org/TR/sparql11-overview/) while preserving established logic programming conventions and features. The following formats are currently supported:
- Input: CSV, TSV, [N-Triples](https://www.w3.org/TR/n-triples/), [Turtle](https://www.w3.org/TR/turtle/), [RDF/XML](https://www.w3.org/TR/rdf-syntax-grammar/)
- Rules: datalog dialect with support for existential rules (tuple-generating dependencies) and datatypes
- Output: CSV

Nemo's datatypes allow the use of RDF-style data values but also "plain" names and constants in any of these formats.

The following [publication](https://github.com/knowsys/nemo/wiki/Publications) gives a first overview of the system and can be used for citing Nemo:

* Alex Ivliev, Stefan Ellmauthaler, Lukas Gerlach, Maximilian Marx, Matthias Meißner, Simon Meusel, Markus Krötzsch: **[Nemo: First Glimpse of a New Rule Engine](https://iccl.inf.tu-dresden.de/web/Inproceedings3354/en).** _Proceedings 39th International Conference on Logic Programming: ICLP 2023 Technical Communications_, 2023. EPTCS, to appear. <a href="https://iccl.inf.tu-dresden.de/web/Inproceedings3354/en">PDF + bibtex</a>

Nemo is in heavy development and the current releases should still be considered unstable.

## Installation

Archives with pre-compiled binaries for various platforms are available from the
[Nemo releases page](https://github.com/knowsys/nemo/releases).
To build your own version from source, you need to have an up-to-date installation of Rust.
Download the source code (from a release or this repository) and run

 `cargo build -r`
 
This will create the command-line client `nmo` in the directory `./target/release/`.

## Usage

Run the following command for an overview of current options:

`nmo --help`

Example Nemo programs and datasets can be found in the [Nemo Examples repository](https://github.com/knowsys/nemo-examples).

## Help

Detailed information for users and developers is found in the [Nemo documentation](https://github.com/knowsys/nemo/wiki/Nemo-documentation).
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

#

*Made with ❤️ in [Dresden](https://www.dresden.de).*
