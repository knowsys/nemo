# syntax=docker/dockerfile:1

# =============================================================================
# Stage 1: Build the Nemo WASM library (nemoWASMBundler)
# =============================================================================
FROM rust:1-bookworm AS wasm-builder

# Install the Rust nightly toolchain pinned by the repo (includes wasm32 target)
COPY rust-toolchain.toml /build/rust-toolchain.toml
WORKDIR /build
RUN rustup show active-toolchain

# Install wasm-pack for building the WASM bindings
RUN cargo install wasm-pack --locked

# Copy the workspace sources needed to build nemo-wasm.
# The workspace Cargo.toml lists all crates as members, so every crate must be
# present for cargo to resolve the workspace.
COPY Cargo.toml Cargo.lock rust-toolchain.toml rustfmt.toml ./
COPY nemo/Cargo.toml nemo/Cargo.toml
COPY nemo/src nemo/src
COPY nemo-cli/Cargo.toml nemo-cli/Cargo.toml
COPY nemo-cli/src nemo-cli/src
COPY nemo-physical/Cargo.toml nemo-physical/Cargo.toml
COPY nemo-physical/src nemo-physical/src
COPY nemo-python/Cargo.toml nemo-python/Cargo.toml
COPY nemo-python/src nemo-python/src
COPY nemo-language-server/Cargo.toml nemo-language-server/Cargo.toml
COPY nemo-language-server/src nemo-language-server/src
COPY nemo-wasm/Cargo.toml nemo-wasm/Cargo.toml
COPY nemo-wasm/src nemo-wasm/src
COPY xtask/Cargo.toml xtask/Cargo.toml
COPY xtask/src xtask/src

# Build the WASM library for both consumers:
#   - nemoWASMBundler (target bundler) is required by nemo-web
#   - nemoWASMWeb (target web) is required by the nemo-vscode-extension
RUN cd nemo-wasm && \
    wasm-pack build --out-dir nemoWASMBundler --target bundler --weak-refs --release && \
    wasm-pack build --out-dir nemoWASMWeb --target web --weak-refs --release

# =============================================================================
# Stage 2: Build the Nemo VSIX extension
# =============================================================================
FROM node:20-bookworm AS vsix-builder

WORKDIR /build/nemo-vscode-extension
COPY nemo-vscode-extension/package.json nemo-vscode-extension/package-lock.json ./
COPY nemo-vscode-extension/src ./src
COPY nemo-vscode-extension/syntaxes ./syntaxes
COPY nemo-vscode-extension/language-configuration.json ./
COPY nemo-vscode-extension/webpack.config.js ./
COPY nemo-vscode-extension/tsconfig.json ./
COPY nemo-vscode-extension/tsconfig.node.json ./
COPY nemo-vscode-extension/.vscodeignore ./

# The extension bundles the WASM web build (nemoWASMWeb) into the VSIX
COPY --from=wasm-builder /build/nemo-wasm/nemoWASMWeb ./nemoWASMWeb

RUN npm ci && npm run package

# =============================================================================
# Stage 3: Build & run the Nemo Web IDE
# =============================================================================
FROM node:20-bookworm AS web

WORKDIR /app

# Copy the nemo-web application sources
COPY nemo-web/package.json nemo-web/package-lock.json ./
COPY nemo-web/index.html ./
COPY nemo-web/vite.config.ts ./
COPY nemo-web/tsconfig.json nemo-web/tsconfig.node.json ./
COPY nemo-web/public ./public
COPY nemo-web/src ./src

# Copy the WASM library and VSIX extension into the locations nemo-web expects
COPY --from=wasm-builder /build/nemo-wasm/nemoWASMBundler ./nemoWASMBundler
COPY --from=vsix-builder /build/nemo-vscode-extension/nemo-*.vsix ./nemo.vsix
RUN mkdir -p nemoVSIX && mv nemo.vsix nemoVSIX/nemo.vsix

# Install dependencies
RUN npm ci

# Expose the Vite dev server port
EXPOSE 5173

# Start the Vite development server, listening on all interfaces
CMD ["npm", "run", "dev", "--", "--host", "0.0.0.0"]
