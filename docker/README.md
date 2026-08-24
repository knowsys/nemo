# Running the Nemo Web IDE with Docker

This setup builds and runs the [Nemo Web IDE](https://github.com/knowsys/nemo-web)
(the browser-based web application for the Nemo rule engine) inside a container.

The web app is a Vite/React single-page application that executes Nemo via
WebAssembly. It requires two artifacts that are built from source as part of the
Docker build:

1. **`nemoWASMBundler`** — the Nemo WASM bindings, built from the `nemo-wasm`
   crate in this repository.
2. **`nemoVSIX/nemo.vsix`** — the Nemo language-server VSIX extension, built
   from the `nemo-vscode-extension` repository.

Both are provided as git submodules so the build is fully self-contained.

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) with the Compose plugin
  (`docker compose`).

## First-time setup

The web app and the VSIX extension are pulled in as git submodules. Initialize
them once:

```bash
git submodule update --init --recursive
```

## Build & run

```bash
docker compose up --build
```

Then open <http://localhost:5173> in your browser.

The first build compiles the Rust WASM library and packages the VSIX extension,
so it can take a while. Subsequent builds are faster thanks to Docker layer
caching.

## Development with live reload

The `docker-compose.yml` bind-mounts the `nemo-web` sources (`src`, `public`,
`index.html`, `vite.config.ts`) into the container, so Vite's hot module
replacement picks up your edits immediately.

> **Note:** If you change the Rust code in `nemo-wasm` or the extension sources,
> you must rebuild the image (`docker compose up --build`) because those are
> baked in at build time.

## Useful commands

| Command | Description |
| --- | --- |
| `docker compose up --build` | Build and start the web server |
| `docker compose up -d` | Start in the background |
| `docker compose down` | Stop and remove the container |
| `docker compose logs -f` | Follow the server logs |
| `docker compose build --no-cache` | Rebuild from scratch (no layer cache) |

## Production build

To create a static production build in `dist` instead of running the dev server:

```bash
docker compose run --rm nemo-web npm run build
```

The output is written to `nemo-web/dist` (bind-mounted via the compose file) and
can be served by any static web server.
