# How to contribute

We are really glad you are reading this, because we need developers to help this project come to fruition.

## Testing

Please make sure to test your additional features.

### Unit-Testing
Unit tests are done for each module by an associated `test` sub-module.
It can either be directly in the `<module.rs>` file or in an additional `test` sub-directory.
Please try to generate meaningful tests, with sane data. It would be most appreciated if there are some real-world flavours.
Add `quickcheck` tests whenever it is applicable.

### Integration-Testing
Integration testing is done in the related `tests` directory on the top-level of this crate.

## Submitting changes

Please send a [GitHub Pull Request to knowsys/nemo](https://github.com/knowsys/nemo/pull/new/main) with a clear list of what you've done (read more about [pull requests](http://help.github.com/pull-requests/)). When you send a pull request be sure to check open and claimed tickets first. We can always use more test coverage. Please follow our coding conventions (below).

Always write a clear log message for your commits. One-line messages are fine for small changes, but bigger changes should have a commit-paragraph and/or a related and appropriately mentioned [issue](https://github.com/knowsys/nemo/issues).

Before creating the pull request be sure to check if
- [ ] all already existing tests are passing,
- [ ] new tests are passing,
- [ ] clippy does not complain about your code, and
- [ ] the code has been formatted with `rustfmt`.

To create a work in progress request, please open the pull request as a draft. (Not applicable in private repository, therefore label the PR as being a `PR-draft`)

### Commit messages

To create uniform and easy to read commit messages, please stick to the following conventions:

  * Capitalise the first word
  * Do not end in punctuation in the message title
  * Use imperative mood
  * The message title shall not exceed 50 characters
  * Be direct, do not use filler words (e.g. "I think", "maybe", "kind of", ...)
  * Use [Github Issue/PR Keywords](https://docs.github.com/en/get-started/writing-on-github/working-with-advanced-formatting/using-keywords-in-issues-and-pull-requests) in the message description part where applicable
  * Link to other related pull requests, issues, commits, comments, ... to have a concise representation of the context in the message description
  * Sign your commit whenever possible

## Coding conventions

Start reading our code and you'll get the hang of it.

  * We use `rustfmt` as code-convention. (you can use whatever styles you like, just let `rustfmt` format the code before you commit)
  * We try to reduce redundancies in enumeration-variant names.
  * We try to use the `where` clause over embedded clauses for better readability
  * We follow the code-conventions and naming-conventions of the current Rust version.
  * We write `clippy`-conform code, so follow `clippy` suggestions where applicable. If you write a compiler-exception (i.e. `#[allow(...)]`) describe your decision to do so in a meaningful comment. We advise to mark this code-segment in the pull-request as a code-comment too. 
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
  * Use `unsafe` code only if :
	* It is checked that there is no safe way to achieve the functionality,
	* it has been discussed with the core development team in detail,
	* the unsafe part is tested even more carefully than the rest of the code, and
	* you will persistently insist on a detailed code-review
  
