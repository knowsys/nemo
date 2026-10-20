/** A WebAssembly module, inlined into the bundle as its bytes. */
declare module "*.wasm" {
  const bytes: Uint8Array;
  export default bytes;
}
