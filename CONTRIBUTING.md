# How to contribute

Nemo is an open project that welcomes contributions. This file contains some hints and guidelines for developers.
Thank you for taking the time to read this! Your input is greatly appreciated.

## Licensing

This project is licensed under either of

- Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or
  https://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or
  https://opensource.org/licenses/MIT)

at the users' option. Unless you explicitly state otherwise, any contribution intentionally submitted for
inclusion in Nemo by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any
additional terms or conditions.

## Where to contribute? What to do?

Reporting problems and suggesting fixes is a great contribution, even if you do not write a line of code!
Detailed [issue reports](https://github.com/knowsys/nemo/issues) with clear descriptions that allow us to 
reproduce the problem and understand what is wanted instead are a huge help. If you have anything that bugs you,
start by reporting it.

To get started with own development, you can check out our [issues](https://github.com/knowsys/nemo/issues), especially
those tagged with "good first issue". If there is no good issue to be found, feel free to contact us for ideas. We
always have some great plans that we did not quite manage to realize ourselves yet ...

If you want to help with an issue, but are not sure how to do it, feel free to start a conversation on the issue.
The same applies if you would like to implement larger changes or new features, since such work should be coordinated
with other ongoing changes. The bigger the feature, the more useful it is to discuss the issue before starting to make
major changes in a branch.

## Submitting changes

Please send a [GitHub Pull Request to knowsys/nemo](https://github.com/knowsys/nemo/pull/new/main) with a clear list of what you've done (read more about [pull requests](http://help.github.com/pull-requests/)). Please follow our coding conventions (see below).

Always write a clear log message for your commits (some hints are below). One-line messages are fine for small changes, but bigger changes should have a commit-paragraph and/or a related and appropriately mentioned [issue](https://github.com/knowsys/nemo/issues).

Before creating the pull request be sure to check if
- [ ] all already existing tests are passing (run `cargo test`),
- [ ] new tests are passing,
- [ ] clippy does not complain about your code (run `cargo clippy`), and
- [ ] the code has been formatted with `rustfmt` (run `cargo fmt`).

To create a work in progress request, please open the pull request as a draft. (Not applicable in private repository, therefore label the PR as being a `PR-draft`)

### Commit messages

The following hints are helpful to create more uniform and readable commit messages (though our own are not perfect either ;-):

  * Capitalise the first word
  * Do not end in punctuation in the message title
  * Use imperative mood
  * The message title shall not exceed 50 characters
  * Be direct, do not use filler words (e.g. "I think", "maybe", "kind of", ...)
  * Use [Github Issue/PR Keywords](https://docs.github.com/en/get-started/writing-on-github/working-with-advanced-formatting/using-keywords-in-issues-and-pull-requests) in the message description part where applicable
  * Link to other related pull requests, issues, commits, comments, ... to have a concise representation of the context in the message description
  * Sign your commit whenever possible

## Testing

Please make sure to test your additional features or fixed bugs.

### Unit testing

Unit tests are done for each module by an associated `test` sub-module.
It can either be directly in the `<module.rs>` file or in an additional `test` sub-directory.
Please try to generate meaningful tests, with sane data. It would be most appreciated if there are some real-world flavours.
Add `quickcheck` tests whenever it is applicable.

### Integration testing

Integration testing is done in the related `tests` directory on the top-level of this crate.

## Coding conventions

Start reading our code and you'll get the hang of it. Code format and essential coding guidelines are already ensured
by our use of `rstufmt` and `clippy` (as mentioned above). Some further conventions are listed below.

  * We try to reduce redundancies in enumeration-variant names.
  * We try to use the `where` clause over embedded clauses for better readability
  * We follow the code-conventions and naming-conventions of the current Rust version.
  * If you write a compiler-exception (i.e. `#[allow(...)]`) describe your decision to do so in a meaningful comment. We advise to mark this code-segment in the pull-request as a code-comment too. 
  * `rustdoc` is obligatory for crate-exposed structures (e.g. `enum`, `struct`, `fn`, ...).
  * `rustdoc` is nice to have for non-crate-exposed structures.
  * We try to have one atomic commit for refactoring work done.
  * Error-handling shall follow these guidelines:
	* Any error that potentially traverses the crate boundary (i.e., anything that does not get handled by the crate itself, or that `panic!`s) is a `crate::error::Error`,
	* an error that is handled by the same module can be a simple `struct`, or an `enum` if there are multiple variants,
	* errors exposed to the rest of the crate have a corresponding variant in `crate::error::Error`, which can be `#[from]` a module-level error `enum`,
	* errors from the standard library have a `#[error(transparent)]` variant in `crate::error::Error`,
	* `panic!` (and `expect()`, `assert!`, `unreachable!` etc.) is fine for situations that should not occur, e.g., if there is some invariant that makes the situation impossible, or where graceful recovery is impossible, but not otherwise, and
	* `unwrap()` should always be `expect("...")` instead.
  * Use `unsafe` code only if:
	* there is no safe way to achieve the functionality/performance,
	* it has been discussed with the core development team in detail,
	* the unsafe part is tested even more carefully than the rest of the code, and
	* you will persistently insist on a detailed code-review
  
