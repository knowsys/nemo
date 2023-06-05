# nemo

This library crate provides all required components for the nemo rule engine. Internally it is based on [nemo-physical](../nemo-physical/README.md), which implements the needed physical database operaions.

You can access the functionality of this crate via the `nmo` command line interface ([nemo-cli](../nemo-cli/README.md)) or through various language bindings ([nemo-python](../nemo-python/README.md), [nemo-wasm](../nemo-wasm/README.md)).

## High level overview

Running a nemo `Program` can be roughly structured into three different phases:

- **Parsing and Analysis** - which forms the frontend of the reasoner. The relevant modules for this are `io::parser` (which contains the parsing logic) and `model` (which contains the definitions for the object model of a parsed program)

- **Execution** - the main module for this is `execution`.

- **Output** - this is implemented in the `io` module.

If you just want an easy to use interface, have a look at the `api` module, which provides convinient access to some common operations.
