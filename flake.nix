rec {
  description = "nemo, a datalog-based rule engine for fast and scalable analytic data processing in memory";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-24.11";
    utils.url = "github:gytis-ivaskevicius/flake-utils-plus";

    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };

    dream2nix = {
      url = "github:nix-community/dream2nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };

    nemo-vscode-extension = {
      url = "github:knowsys/nemo-vscode-extension";
      inputs = {
        nixpkgs.follows = "nixpkgs";
        utils.follows = "utils";
        dream2nix.follows = "dream2nix";
      };
    };

    nemo-web = {
      url = "github:knowsys/nemo-web";
      inputs = {
        nixpkgs.follows = "nixpkgs";
        utils.follows = "utils";
        dream2nix.follows = "dream2nix";
        nemo.follows = "nemo-vscode-extension/nemo";
      };
    };

    nemo-doc = {
      url = "github:knowsys/nemo-doc";
      inputs = {
        nixpkgs.follows = "nixpkgs";
        utils.follows = "utils";
        dream2nix.follows = "dream2nix";
      };
    };
  };

  outputs =
    inputs@{
      self,
      utils,
      rust-overlay,
      ...
    }:
    utils.lib.mkFlake {
      inherit self inputs;

      channels.nixpkgs.overlaysBuilder = channels: [
        rust-overlay.overlays.default
      ];

      overlays = {
        rust-overlay = rust-overlay.overlays.default;

        default =
          final: prev:
          let
            pkgs = self.packages."${final.system}";
            lib = self.channels.nixpkgs."${final.system}";
          in
          {
            inherit (pkgs)
              nemo
              nemo-language-server
              nemo-python
              nemo-wasm
              nemo-wasm-web
              nemo-wasm-bundler
              nemo-wasm-node
              nemo-web
              nemo-vscode-extension
              ;

            nodePackages = lib.makeExtensible (lib.extends pkgs.nodePackages prev.nodePackages);
          };

        nemo-vscode-extension = inputs.nemo-vscode-extension.overlays.default;
        nemo-web = inputs.nemo-web.overlays.default;
      };

      outputsBuilder =
        channels:
        let
          pkgs = channels.nixpkgs;
          toolchain = pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;
          platform = pkgs.makeRustPlatform {
            cargo = toolchain;
            rustc = toolchain;
          };
          defaultBuildInputs =
            [
              pkgs.openssl
              pkgs.openssl.dev
            ]
            ++ pkgs.lib.optionals pkgs.stdenv.isDarwin [
              pkgs.darwin.apple_sdk.frameworks.Security
              pkgs.darwin.apple_sdk.frameworks.SystemConfiguration
            ];
          defaultNativeBuildInputs = [
            toolchain
            pkgs.pkg-config
          ];

          runCargo' =
            name: env: buildCommand:
            pkgs.stdenv.mkDerivation (
              {
                preferLocalBuild = true;
                allowSubstitutes = false;

                RUSTFLAGS = "-Dwarnings";
                RUSTDOCFLAGS = "-Dwarnings";

                src = ./.;
                cargoDeps = platform.importCargoLock { lockFile = ./Cargo.lock; };

                buildInputs = defaultBuildInputs;
                nativeBuildInputs =
                  defaultNativeBuildInputs
                  ++ (with platform; [
                    cargoSetupHook
                    pkgs.python3
                  ]);

                inherit name;

                buildPhase = ''
                  runHook preBuild
                  mkdir $out
                  ${buildCommand}
                  runHook postBuild
                '';
              }
              // env
            );
          runCargo = name: runCargo' name { };
        in
        rec {
          packages =
            let
              cargoMeta = (builtins.fromTOML (builtins.readFile ./Cargo.toml)).workspace.package;
              inherit (cargoMeta) version;
              meta = {
                inherit description;
                inherit (cargoMeta) homepage;
                license = [
                  pkgs.lib.licenses.asl20
                  pkgs.lib.licenses.mit
                ];
              };

              buildWasm =
                target:
                {
                  pname,
                  src,
                  version,
                  meta,
                  ...
                }@args:
                pkgs.stdenv.mkDerivation rec {
                  inherit (args)
                    version
                    meta
                    pname
                    src
                    ;

                  cargoDeps = platform.importCargoLock { lockFile = ./Cargo.lock; };

                  nativeBuildInputs =
                    defaultNativeBuildInputs
                    ++ (with platform; [
                      cargoSetupHook
                      pkgs.wasm-pack
                      pkgs.nodejs
                      pkgs.wasm-bindgen-cli
                    ]);
                  buildInputs = defaultBuildInputs ++ [ pkgs.nodejs ];

                  buildPhase = ''
                    runHook preBuild

                    mkdir -p $out/lib/node_modules/nemo-wasm
                    mkdir .cache
                    mkdir target
                    export CARGO_HOME=$TMPDIR/.cargo
                    export CARGO_TARGET_DIR=$TMPDIR/target
                    export XDG_CACHE_HOME=$TMPDIR/.cache

                    cd $src
                    HOME=$TMPDIR wasm-pack build --target ${target} --weak-refs --mode=no-install --out-dir=$out/lib/node_modules/${pname} nemo-wasm

                    runHook postBuild
                  '';
                };

              manpages = runCargo "nemo-generate-manpages" "cargo xtask generate-manpages $out";
              shellCompletions = runCargo "nemo-generate-shell-completions" "cargo xtask generate-shell-completions $out";
            in
            rec {
              nemo = platform.buildRustPackage {
                pname = "nemo";
                src = ./.;
                meta = meta // {
                  mainProgram = "nmo";
                };
                inherit version;

                cargoLock.lockFile = ./Cargo.lock;

                buildInputs = defaultBuildInputs;
                nativeBuildInputs =
                  defaultNativeBuildInputs
                  ++ (with platform; [
                    cargoBuildHook
                    cargoCheckHook
                    pkgs.installShellFiles
                  ]);
                buildAndTestSubdir = "nemo-cli";

                postInstall = ''
                  installManPage ${manpages}/nmo.1
                  installShellCompletion \
                    --fish ${shellCompletions}/nmo.fish \
                    --bash ${shellCompletions}/nmo.bash \
                    --zsh ${shellCompletions}/_nmo
                '';
              };

              nemo-language-server = platform.buildRustPackage {
                pname = "nemo-language-server";
                src = ./.;
                meta = meta // {
                  mainProgram = "nemo-language-server";
                };
                inherit version;

                cargoLock.lockFile = ./Cargo.lock;

                buildInputs = defaultBuildInputs;
                nativeBuildInputs =
                  defaultNativeBuildInputs
                  ++ (with platform; [
                    cargoBuildHook
                    cargoCheckHook
                  ]);
                buildAndTestSubdir = "nemo-language-server";
              };

              nemo-python = pkgs.python3Packages.buildPythonPackage {
                pname = "nemo-python";
                src = ./.;
                inherit version meta;

                cargoDeps = platform.importCargoLock { lockFile = ./Cargo.lock; };

                buildInputs = defaultBuildInputs;
                nativeBuildInputs =
                  defaultNativeBuildInputs
                  ++ (with platform; [
                    cargoSetupHook
                    maturinBuildHook
                  ]);
                buildAndTestSubdir = "nemo-python";

                checkPhase = ''
                  PYTHONPATH=''${PYTHONPATH:+''${PYTHONPATH}:}out python3 -m unittest discover -s nemo-python/tests -v
                '';
              };

              nemo-wasm-node = buildWasm "nodejs" {
                pname = "nemo-wasm";
                src = ./.;
                inherit version meta;
              };
              nemo-wasm-bundler = buildWasm "bundler" {
                pname = "nemo-wasm";
                src = ./.;
                inherit version meta;
              };
              nemo-wasm-web = buildWasm "web" {
                pname = "nemo-wasm";
                src = ./.;
                inherit version meta;
              };
              nemo-wasm = nemo-wasm-bundler;

              python3 = pkgs.python3.withPackages (ps: [ nemo-python ]);
              python = python3;

              nodejs = pkgs.writeShellScriptBin "node" ''
                NODE_PATH=${nemo-wasm-node}/lib/node_modules''${NODE_PATH:+":$NODE_PATH"} ${pkgs.nodejs}/bin/node $@
              '';

              nemo-vscode-extension-vsix =
                (inputs.nemo-vscode-extension.packages.${pkgs.system}.nemo-vscode-extension-vsix.extendModules {
                  modules = [
                    {
                      deps = {
                        inherit nemo-wasm-web;
                      };
                    }
                  ];
                }).config.public;

              nemo-vscode-extension =
                inputs.nemo-vscode-extension.packages.${pkgs.system}.nemo-vscode-extension.overrideAttrs
                  (old: {
                    src = "${nemo-vscode-extension-vsix}/nemo-${old.version}.vsix";
                  });

              nemo-web =
                (inputs.nemo-web.packages.${pkgs.system}.nemo-web.extendModules {
                  modules = [
                    {
                      deps = {
                        inherit nemo-wasm-web nemo-wasm-bundler nemo-vscode-extension-vsix;
                      };
                    }
                  ];
                }).config.public;

              inherit (inputs.nemo-doc.packages.${pkgs.system}) nemo-doc;

              default = nemo;
            };

          apps = {
            nemo-web = utils.lib.mkApp {
              drv = pkgs.writeShellApplication {
                name = "nemo-web-preview";

                runtimeInputs = [
                  pkgs.nodejs
                ];

                text = ''
                  cd "$(mktemp --directory)"
                  cp -R ${self.packages.${pkgs.system}.nemo-web}/lib/node_modules/nemo-web/* .
                  mkdir wrapper
                  ln -s ../node_modules/vite/bin/vite.js wrapper/vite
                  export PATH="''${PATH}:wrapper"
                  npm run preview
                '';
              };
            };

            nemo-doc = inputs.nemo-doc.apps.${pkgs.system}.nemo-doc-preview;

            ci-checks = utils.lib.mkApp {
              drv = pkgs.writeShellApplication {
                name = "nemo-run-ci-checks";

                runtimeInputs = pkgs.lib.concatLists [
                  defaultBuildInputs
                  defaultNativeBuildInputs
                  [
                    pkgs.python3
                    pkgs.python3Packages.pycodestyle
                    pkgs.maturin
                    pkgs.wasm-pack
                    pkgs.wasm-bindgen-cli
                  ]
                ];

                text = ''
                  export RUSTFLAGS"=-Dwarnings"
                  export RUSTDOCFLAGS="-Dwarnings"

                  if [[ ! -f "flake.nix" || ! -f "Cargo.toml" || ! -f "rust-toolchain.toml" ]]; then
                    echo "This should be run from the top-level of the nemo source tree."
                    exit 1
                  fi

                  cargo test
                  cargo clippy --all-targets
                  cargo fmt --all -- --check
                  cargo doc --workspace

                  pushd nemo-python
                    pycodestyle .
                    VENV="$(mktemp -d)"
                    python3 -m venv "''${VENV}"
                    # shellcheck disable=SC1091
                    . "''${VENV}"/bin/activate
                    maturin develop
                    python3 -m unittest discover tests -v
                    deactivate
                    rm -rf "''${VENV}"
                  popd

                  HOME=$TMPDIR wasm-pack build --weak-refs --mode=no-install nemo-wasm

                  cargo miri test
                '';
              };
            };
          };

          checks = {
            inherit (packages)
              nemo
              nemo-language-server
              nemo-python
              nemo-wasm
              nemo-wasm-node
              nemo-wasm-web
              nemo-web
              nemo-doc
              nemo-vscode-extension
              ;
            devShell = devShells.default;

            clippy = runCargo "nemo-check-clippy" ''
              cargo clippy --all-targets
            '';

            doc = runCargo "nemo-check-docs" ''
              cargo doc --workspace
            '';

            fmt = runCargo "nemo-check-formatting" ''
              cargo fmt --all -- --check
            '';

            test = runCargo "nemo-check-tests" ''
              cargo test
            '';

            python-codestyle =
              pkgs.runCommandLocal "nemo-check-python-codestyle"
                {
                  nativeBuildInputs = [ pkgs.python3Packages.pycodestyle ];
                }
                ''
                  mkdir $out
                  pycodestyle ${./nemo-python}
                '';
          };

          devShells.default = pkgs.mkShell {
            NMO_LOG = "debug";
            RUST_LOG = "debug";
            RUST_BACKTRACE = 1;
            RUST_SRC_PATH = "${toolchain}/lib/rustlib/src/rust/library";

            shellHook = ''
              export PATH=''${HOME}/.cargo/bin''${PATH+:''${PATH}}
            '';

            buildInputs = pkgs.lib.concatLists [
              defaultBuildInputs
              defaultNativeBuildInputs
              [
                pkgs.cargo-audit
                pkgs.cargo-license
                pkgs.cargo-tarpaulin
                pkgs.gnuplot
                pkgs.maturin
                pkgs.python3
                pkgs.wasm-pack
                pkgs.wasm-bindgen-cli
                pkgs.nodejs
              ]
            ];
          };

          formatter = channels.nixpkgs.nixfmt-rfc-style;
        };
    };
}
