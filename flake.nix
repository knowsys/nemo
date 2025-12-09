{
  description = "nemo, a datalog-based rule engine for fast and scalable analytic data processing in memory";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.05";
    utils.url = "github:gytis-ivaskevicius/flake-utils-plus";

    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };

    crane.url = "github:ipetkov/crane";

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

              nemo-no-incremental
              nemo-only-incremental
              nemo-no-merge
              nemo-no-cartesian
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
          inherit (pkgs) lib system;
          toolchain = pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;

          crane = (inputs.crane.mkLib pkgs).overrideToolchain toolchain;
          src = lib.fileset.toSource rec {
            root = ./.;
            fileset = lib.fileset.unions [
              (crane.fileset.commonCargoSources root)
              (lib.fileset.fileFilter (
                file:
                lib.any file.hasExt [
                  "md"
                  "py"
                ]
              ) root)
              ./resources # contains the integration tests
              ./LICENSE-MIT
              ./LICENSE-APACHE
            ];
          };

          inherit (pkgs) python3; # this should match the install hook version below
          inherit (pkgs.python3Packages) pypaInstallHook pycodestyle; # this should match the python version above

          commonArgs = {
            inherit src;
            strictDeps = true;

            nativeBuildInputs = lib.attrValues {
              inherit python3;
              inherit (pkgs) pkg-config;
            };

            env = {
              # avoid rebuilds by pinning python:
              # https://crane.dev/faq/rebuilds-pyo3.html
              PYO3_PYTHON = lib.getExe python3;

              # thresholds for the time reporting for cargo test
              # first value is warning, second is critical
              # values are in milliseconds
              RUST_TEST_TIME_UNIT = "15000,60000";
              RUST_TEST_TIME_INTEGRATION = "15000,60000";
              RUST_TEST_TIME_DOCTEST = "15000,60000";
            };

            buildInputs =
              (lib.attrValues { inherit (pkgs) openssl; })
              ++ lib.optionals pkgs.stdenv.isDarwin (
                lib.attrValues {
                  inherit (pkgs) libiconv;
                  inherit (pkgs.darwin.apple_sdk.frameworks) Security SystemConfiguration;
                }
              );
          };

          # build all dependencies in a single derivation, so that they can be cached
          cargoArtifacts =
            let
              hack =
                {
                  task,
                  profile ? "release",
                  target ? null,
                }:
                "cargo hack --workspace --feature-powerset ${task} --profile ${profile} ${
                  lib.optionalString (target != null) "--target ${target}"
                }";
            in
            crane.buildDepsOnly (
              commonArgs
              // {
                pname = "nemo-cargo-workspace";
                cargoBuildCommand = hack { task = "build"; };
                cargoCheckCommand = hack { task = "build"; };
                cargoTestCommand = hack { task = "test"; };

                nativeBuildInputs = commonArgs.nativeBuildInputs ++ (lib.attrValues { inherit (pkgs) cargo-hack; });
              }
            );

          # the same, but for the WASM target
          cargoWasmArtifacts = crane.buildDepsOnly (
            commonArgs
            // {
              pname = "nemo-cargo-workspace-wasm";
              cargoExtraArgs = "--target wasm32-unknown-unknown --package nemo-wasm --package nemo-language-server --no-default-features --features js";

              doCheck = false; # skip tests for WASM, since they wouldn't run anyways
            }
          );

          crateArgs = commonArgs // {
            inherit cargoArtifacts;
            inherit (crane.crateNameFromCargoToml { inherit src; }) version;

            meta = {
              inherit ((builtins.fromTOML (builtins.readFile ./Cargo.toml)).workspace.package)
                description
                homepage
                ;
              license = [
                pkgs.lib.licenses.asl20
                pkgs.lib.licenses.mit
              ];
            };
          };

          cargoXtask =
            {
              task,
              cargoXtaskArgs ? "",
              ...
            }@origArgs:
            let
              args = builtins.removeAttrs origArgs [
                "task"
                "cargoXtaskArgs"
              ];
            in
            crane.mkCargoDerivation (
              args
              // {
                inherit cargoArtifacts src;
                pname = "nemo-xtask-${task}";
                buildPhaseCargoCommand = "cargo run --release --package xtask -- ${task} ${cargoXtaskArgs}";
                buildInputs = args.buildInputs or commonArgs.buildInputs;
                nativeBuildInputs = args.nativeBuildInputs or commonArgs.nativeBuildInputs;
              }
            );

          cargoXtaskGenerate =
            task:
            cargoXtask {
              task = "generate-${task}";
              cargoXtaskArgs = "$out";
              preBuild = ''
                mkdir $out
              '';
            };

          buildCrate =
            {
              crate,
              pname ? crate,
              ...
            }@origArgs:
            let
              args = builtins.removeAttrs origArgs [
                "crate"
              ];
            in
            crane.buildPackage (
              crateArgs
              // {
                inherit pname;
                cargoExtraArgs = "${args.cargoExtraArgs or ""} --package ${crate}";
              }
              // args
            );

          wasmPack =
            { target, ... }@origArgs:
            let
              args = builtins.removeAttrs origArgs [
                "target"
              ];
            in
            crane.mkCargoDerivation (
              args
              // {
                inherit src;
                cargoArtifacts = cargoWasmArtifacts;
                pname = "nemo-wasm-${target}";

                buildPhaseCargoCommand = ''
                  cat LICENSE-APACHE LICENSE-MIT > nemo-wasm/LICENSE || true
                  mkdir -p lib/node_modules/nemo-wasm
                  wasm-pack build \
                    --target ${target} \
                    --weak-refs \
                    --mode=no-install \
                    --out-dir=$TMPDIR/lib/node_modules/nemo-wasm nemo-wasm
                '';

                installPhaseCommand = ''
                  mkdir -p $out
                  cp -R $TMPDIR/lib $out/
                '';

                buildInputs = (args.buildInputs or [ ]) ++ crateArgs.buildInputs;
                nativeBuildInputs =
                  (args.nativeBuildInputs or [ ])
                  ++ crateArgs.nativeBuildInputs
                  ++ (lib.attrValues {
                    inherit (pkgs)
                      binaryen
                      wasm-bindgen-cli_0_2_100
                      wasm-pack
                      writableTmpDirAsHomeHook
                      ;
                  });
              }
            );

          nemo-cli-manpages = cargoXtaskGenerate "manpages";
          nemo-cli-shell-completions = cargoXtaskGenerate "shell-completions";
        in
        rec {
          packages =
            let
              inherit (lib) optionalString;
              buildNemoCrate =
                {
                  withSN ? true,
                  withEC ? true,
                  withMT ? true,
                }:
                let
                  SN = if withSN then "SN" else "noSN";
                  EC = if withEC then "EC" else "noEC";
                  MT = if withMT then "MT" else "noMT";
                  variant = "${SN}-${EC}-${MT}";
                  binary = "nmo-${variant}";
                in
                buildCrate {
                  pname = "nemo-${variant}";
                  crate = "nemo-cli";

                  cargoExtraArgs = builtins.concatStringsSep " " [
                    "-p nemo-cli"
                    (optionalString withSN "--features nemo/import-incremental")
                    (optionalString withEC "--features nemo/import-cartesian")
                    (optionalString withMT "--features nemo/import-merge")
                  ];

                  nativeBuildInputs =
                    crateArgs.nativeBuildInputs ++ (lib.attrValues { inherit (pkgs) installShellFiles; });

                  postInstall = ''
                    mv -n $out/bin/nmo $out/bin/${binary}                    
                    installShellCompletion \
                      --fish --name ${binary}.fish ${nemo-cli-shell-completions}/nmo.fish \
                      --bash --name ${binary}.bash ${nemo-cli-shell-completions}/nmo.bash \
                      --zsh --name _${binary} ${nemo-cli-shell-completions}/_nmo
                    cp ${nemo-cli-manpages}/nmo.1 ${binary}.1
                    installManPage ${binary}.1
                  '';

                  meta = crateArgs.meta // {
                    mainProgram = binary;
                  };
                };
            in
            rec {
              nemo = buildCrate {
                pname = "nemo";
                crate = "nemo-cli";

                nativeBuildInputs =
                  crateArgs.nativeBuildInputs ++ (lib.attrValues { inherit (pkgs) installShellFiles; });

                postInstall = ''
                  installManPage ${nemo-cli-manpages}/nmo.1
                  installShellCompletion \
                    --fish ${nemo-cli-shell-completions}/nmo.fish \
                    --bash ${nemo-cli-shell-completions}/nmo.bash \
                    --zsh  ${nemo-cli-shell-completions}/_nmo
                '';

                meta = crateArgs.meta // {
                  mainProgram = "nmo";
                };
              };

              nemo-noSN-noEC-noMT = buildNemoCrate {
                withSN = false;
                withEC = false;
                withMT = false;
              };
              nemo-SN-noEC-noMT = buildNemoCrate {
                withSN = true;
                withEC = false;
                withMT = false;
              };
              nemo-SN-noEC-MT = buildNemoCrate {
                withSN = true;
                withEC = false;
                withMT = true;
              };
              nemo-SN-EC-noMT = buildNemoCrate {
                withSN = true;
                withEC = true;
                withMT = false;
              };
              nemo-SN-EC-MT = buildNemoCrate {
                withSN = true;
                withEC = true;
                withMT = true;
              };

              docker = pkgs.dockerTools.buildImage {
                name = "nemo";
                tag = "latest";
                copyToRoot = pkgs.buildEnv {
                  name = "image-root";
                  paths = [
                    pkgs.cacert
                    pkgs.openssl
                    nemo
                  ];
                };
                config = {
                  Entrypoint = [ "/bin/nmo" ];
                };
              };

              nemo-language-server = buildCrate {
                crate = "nemo-language-server";
              };

              nemo-python = buildCrate {
                crate = "nemo-python";

                doInstallCheck = true;
                doNotPostBuildInstallCargoBinaries = true;

                nativeBuildInputs =
                  crateArgs.nativeBuildInputs
                  ++ (lib.attrValues {
                    inherit pypaInstallHook;
                    inherit (pkgs) maturin;
                  });

                buildPhaseCargoCommand = ''
                  maturin build \
                    --offline \
                    --target-dir target \
                    --manylinux off \
                    --strip \
                    --release \
                    --manifest-path nemo-python/Cargo.toml
                '';

                preInstall = ''
                  mkdir -p dist
                  cp target/wheels/*.whl dist/
                '';

                installPhaseCommand = "pypaInstallPhase";

                installCheckPhase = ''
                  PYTHONPATH=''${PYTHONPATH:+''${PYTHONPATH}:}out python3 -m unittest discover -s nemo-python/tests -v
                '';
              };

              nemo-wasm-node = wasmPack { target = "nodejs"; };
              nemo-wasm-bundler = wasmPack { target = "bundler"; };
              nemo-wasm-web = wasmPack { target = "web"; };
              nemo-wasm = nemo-wasm-bundler;

              python3 = pkgs.python3.withPackages (ps: [ nemo-python ]);
              python = python3;

              nodejs = pkgs.writeShellApplication {
                name = "node";
                meta = { inherit (pkgs.nodejs.meta) description; };

                runtimeInputs = lib.attrValues { inherit (pkgs) nodejs; };

                text = ''
                  NODE_PATH=${nemo-wasm-node}/lib/node_modules''${NODE_PATH:+":$NODE_PATH"} node "$@"
                '';
              };

              nemo-vscode-extension-vsix =
                (inputs.nemo-vscode-extension.packages.${system}.nemo-vscode-extension-vsix.extendModules {
                  modules = [
                    {
                      deps = {
                        inherit nemo-wasm-web;
                      };
                    }
                  ];
                }).config.public;

              nemo-vscode-extension =
                inputs.nemo-vscode-extension.packages.${system}.nemo-vscode-extension.overrideAttrs
                  (old: {
                    src = "${nemo-vscode-extension-vsix}/nemo-${old.version}.vsix";
                  });

              nemo-web =
                (inputs.nemo-web.packages.${system}.nemo-web.extendModules {
                  modules = [
                    {
                      deps = {
                        inherit nemo-wasm-web nemo-wasm-bundler nemo-vscode-extension-vsix;
                      };
                    }
                  ];
                }).config.public;

              inherit (inputs.nemo-doc.packages.${system}) nemo-doc;

              default = nemo;
            };

          apps = {
            nemo-web = utils.lib.mkApp {
              drv = pkgs.writeShellApplication {
                name = "nemo-web-preview";

                runtimeInputs = lib.attrValues { inherit (pkgs) nodejs; };

                text = ''
                  cd "$(mktemp --directory)"
                  cp -R ${self.packages.${pkgs.system}.nemo-web}/lib/node_modules/nemo-web/* .
                  chmod -R +w node_modules
                  mkdir wrapper
                  ln -s ${
                    self.packages.${pkgs.system}.nemo-web
                  }/lib/node_modules/nemo-web/node_modules/vite/bin/vite.js wrapper/vite
                  export PATH="''${PATH}:wrapper"
                  npm run preview
                '';
              };
            };

            nemo-doc = inputs.nemo-doc.apps.${system}.nemo-doc-preview;

            ci-checks = utils.lib.mkApp {
              drv = pkgs.writeShellApplication {
                name = "nemo-run-ci-checks";

                runtimeInputs = lib.concatLists [
                  self.devShells.${system}.default.buildInputs
                  self.devShells.${system}.default.nativeBuildInputs
                ];

                text = ''
                  export RUSTFLAGS"=--deny warnings"
                  export RUSTDOCFLAGS="--deny warnings"
                  export PYO3_PYTHON="${commonArgs.env.PYO3_PYTHON}"

                  if [[ ! -f "flake.nix" || ! -f "Cargo.toml" || ! -f "rust-toolchain.toml" ]]; then
                    echo "This should be run from the top-level of the nemo source tree."
                    exit 1
                  fi

                  cargo test --workspace --all-targets -- -Z unstable-options --report-time
                  cargo clippy --workspace --all-targets -- --deny warnings
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

                  RUSTFLAGS="" wasm-pack build --weak-refs --mode=no-install nemo-wasm

                  cargo miri test -- -Z unstable-options --report-time
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

            clippy = crane.cargoClippy (
              commonArgs
              // {
                pname = "nemo-cargo-workspace";
                inherit cargoArtifacts;
                cargoClippyExtraArgs = "--workspace --all-targets -- --deny warnings";
              }
            );

            doc = crane.cargoDoc (
              commonArgs
              // {
                pname = "nemo-cargo-workspace";
                inherit cargoArtifacts;
                env.RUSTDOCFLAGS = "--deny warnings";
                cargoDocExtraArgs = "--workspace";
              }
            );

            fmt = crane.cargoFmt (
              commonArgs
              // {
                pname = "nemo-cargo-workspace";
              }
            );

            test = crane.cargoTest (
              commonArgs
              // {
                pname = "nemo-cargo-workspace";
                inherit cargoArtifacts;
                cargoTestExtraArgs = "--workspace --all-targets";
              }
            );

            python-codestyle =
              pkgs.runCommandLocal "nemo-check-python-codestyle"
                {
                  nativeBuildInputs = [ pycodestyle ];
                }
                ''
                  mkdir $out
                  pycodestyle ${./nemo-python}
                '';
          };

          devShells.default = crane.devShell {
            checks = lib.filterAttrs (
              name: _value:
              builtins.elem name [
                "nemo-web"
                "nemo-doc"
              ]
            ) self.checks.${system};

            NMO_LOG = "debug";
            RUST_LOG = "debug";
            RUST_BACKTRACE = 1;
            RUST_SRC_PATH = "${toolchain}/lib/rustlib/src/rust/library";
            inherit (commonArgs.env)
              PYO3_PYTHON
              RUST_TEST_TIME_UNIT
              RUST_TEST_TIME_INTEGRATION
              RUST_TEST_TIME_DOCTEST
              ;

            shellHook = ''
              export PATH=''${HOME}/.cargo/bin''${PATH+:''${PATH}}
            '';

            packages = lib.concatLists [
              crateArgs.nativeBuildInputs
              crateArgs.buildInputs

              (lib.attrValues {
                inherit
                  python3
                  pycodestyle
                  ;
                inherit (pkgs)
                  cargo-audit
                  cargo-license
                  cargo-tarpaulin

                  maturin
                  binaryen
                  wasm-bindgen-cli
                  wasm-pack

                  gnuplot
                  nodejs
                  ;
              })
            ];
          };

          formatter = channels.nixpkgs.nixfmt-rfc-style;
        };
    };
}
