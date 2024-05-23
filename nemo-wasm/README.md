# nemo-wasm

This crate provides a Web Assembly build and JavaScript/TypeScript bindings for the `nemo` crate.

> **Note**
> These bindings are currently in an experimental state and subject to change.

## Building

- Install [wasm-pack](https://rustwasm.github.io/wasm-pack/book/prerequisites/index.html)
- Build the library:

```
wasm-pack build --out-dir nemoWASMBundler --target bundler --weak-refs --release
wasm-pack build --out-dir nemoWASMWeb --target web --weak-refs --release
```

- In order to use the `FileSystemSyncAccessHandle` APIs, the `web_sys_unstable_apis` `cfg` flag needs to be set
  - See https://rustwasm.github.io/docs/wasm-bindgen/web-sys/unstable-apis.html
  - See https://rustwasm.github.io/wasm-bindgen/api/web_sys/struct.FileSystemSyncAccessHandle.html

## Example usage

### TypeScript

```typescript
const program = new NemoProgram(programText);
console.log(program.getEDBPredicates());

const engine = new NemoEngine(program);
engine.reason();

for (const predicate of program.getOutputPredicates()) {
  const rows = new NemoResultsIterable(engine.getResult(predicate));

  for (const row of rows) {
    console.log(row);
  }
}

// See https://developer.mozilla.org/en-US/docs/Web/JavaScript/Guide/Iterators_and_Generators
// Iterables are not directly supported yet, see https://github.com/rustwasm/wasm-bindgen/issues/1478
class NemoResultsIterable {
  public constructor(private iterator: NemoResults) {}

  public [Symbol.iterator]() {
    return this.iterator;
  }
}
```

### JavaScript

```typescript
const program = new NemoProgram(programText);
console.log(program.getEDBPredicates());

const engine = new NemoEngine(program);
engine.reason();

for (const predicate of program.getOutputPredicates()) {
  const rows = new NemoResultsIterable(engine.getResult(predicate));

  for (const row of rows) {
    console.log(row);
  }
}

// See https://developer.mozilla.org/en-US/docs/Web/JavaScript/Guide/Iterators_and_Generators
// Iterables are not directly supported yet, see https://github.com/rustwasm/wasm-bindgen/issues/1478
class NemoResultsIterable {
  constructor(iterator) {
    this.iterator = iterator;
  }

  [Symbol.iterator]() {
    return this.iterator;
  }
}
```
