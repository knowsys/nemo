{
  description = "nemo, a datalog-based rule engine for fast and scalable analytic data processing in memory";

  inputs = {
    nixpkgs.url = "https://channels.nixos.org/nixos-26.05/nixexprs.tar.xz";

    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };

    crane.url = "github:ipetkov/crane";

    dream2nix = {
      url = "github:nix-community/dream2nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };

    nemo-web = {
      url = "github:knowsys/nemo-web/lazer";
      inputs = {
        nixpkgs.follows = "nixpkgs";
        dream2nix.follows = "dream2nix";
        # this flake, which also provides the editor extension
        nemo.follows = "";
      };
    };

    nemo-doc = {
      url = "github:knowsys/nemo-doc";
      inputs = {
        nixpkgs.follows = "nixpkgs";
        dream2nix.follows = "dream2nix";
      };
    };
  };

  outputs =
    inputs@{
      self,
      nixpkgs,
      rust-overlay,
      ...
    }:
    let
      inherit (nixpkgs) lib;
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "aarch64-darwin"
        "x86_64-darwin"
      ];
      forAllSystems' = systems: f: lib.genAttrs systems f;
      forAllSystems = forAllSystems' systems;

      perSystem =
        system:
        let
          pkgs = import nixpkgs {
            overlays = [ rust-overlay.overlays.default ];
            inherit system;
          };
          toolchain = pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;

          crane = (inputs.crane.mkLib pkgs).overrideToolchain toolchain;
          src = lib.fileset.toSource rec {
            root = ./.;
            fileset = lib.fileset.unions [
              (crane.fileset.commonCargoSources root)
              (lib.fileset.fileFilter (
                file:
                lib.any file.hasExt [
                  "html" # included into the timing report
                  "md"
                  "py"
                  "pyi"
                ]
              ) root)
              ./resources # contains the integration tests
              ./LICENSE-MIT
              ./LICENSE-APACHE
            ];
          };

          inherit (pkgs) python3; # this should match the install hook version below
          inherit (pkgs.python3Packages) pypaInstallHook pycodestyle pytest; # this should match the python version above
          # find correct wasm-bindgen-version
          wasm-bindgen =
            let
              version = lib.pipe ./Cargo.lock [
                builtins.readFile
                fromTOML
                (d: d.package)
                (lib.findSingle (d: d.name == "wasm-bindgen") { } { })
                (d: d.version)
                (lib.replaceString "." "_")
              ];
            in
            pkgs.${"wasm-bindgen-cli_${version}"};

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
                # the language server's two runtimes cannot share a build
                "cargo hack --workspace --feature-powerset --mutually-exclusive-features stdio,wasm ${task} --profile ${profile} ${
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
              cargoExtraArgs = "--target wasm32-unknown-unknown --package nemo-wasm";

              doCheck = false; # skip tests for WASM, since they wouldn't run anyways
            }
          );

          crateArgs = commonArgs // {
            inherit cargoArtifacts;
            inherit (crane.crateNameFromCargoToml { inherit src; }) version;

            meta = {
              inherit ((fromTOML (builtins.readFile ./Cargo.toml)).workspace.package)
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
              args = removeAttrs origArgs [
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
              args = removeAttrs origArgs [
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
              args = removeAttrs origArgs [
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
                    inherit wasm-bindgen;
                    inherit (pkgs)
                      binaryen
                      wasm-pack
                      writableTmpDirAsHomeHook
                      ;
                  });
              }
            );

          vscodeExtensionMeta = lib.importJSON ./nemo-vscode/package.json;

          nemo-cli-manpages = cargoXtaskGenerate "manpages";
          nemo-cli-shell-completions = cargoXtaskGenerate "shell-completions";
        in
        {
          packages = {
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

            docker = pkgs.dockerTools.buildImage {
              name = "nemo";
              tag = "latest";
              copyToRoot = pkgs.buildEnv {
                name = "image-root";
                paths = [
                  pkgs.cacert
                  pkgs.openssl
                  self.packages.${system}.nemo
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
                  inherit pypaInstallHook pytest;
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
                PYTHONPATH=$out/${python3.sitePackages}''${PYTHONPATH:+:''${PYTHONPATH}} python3 -m pytest nemo-python/tests -q
              '';
            };

            nemo-wasm-node = wasmPack { target = "nodejs"; };
            nemo-wasm-bundler = wasmPack { target = "bundler"; };
            nemo-wasm-web = wasmPack { target = "web"; };
            nemo-wasm = self.packages.${system}.nemo-wasm-bundler;

            python3 =
              (pkgs.python3.override {
                packageOverrides = pyfinal: pyprev: {
                  nemo-python = pyfinal.toPythonModule self.packages.${system}.nemo-python;
                };
              }).withPackages
                (ps: [ ps.nemo-python ]);
            python = self.packages.${system}.python3;

            nodejs = pkgs.writeShellApplication {
              name = "node";
              meta = { inherit (pkgs.nodejs.meta) description; };

              runtimeInputs = lib.attrValues { inherit (pkgs) nodejs; };

              text = ''
                NODE_PATH=${
                  self.packages.${system}.nemo-wasm-node
                }/lib/node_modules''${NODE_PATH:+":$NODE_PATH"} node "$@"
              '';
            };

            nemo-vscode-extension-vsix = inputs.dream2nix.lib.evalModules {
              packageSets.nixpkgs = pkgs;

              modules = [
                {
                  paths = {
                    projectRoot = ./.;
                    projectRootFile = "flake.nix";
                    package = ./nemo-vscode;
                  };
                }

                (
                  {
                    config,
                    dream2nix,
                    ...
                  }:
                  {
                    name = "nemo-vscode-extension-vsix";
                    inherit (vscodeExtensionMeta) version;

                    imports = [
                      dream2nix.modules.dream2nix.nodejs-package-lock-v3
                      dream2nix.modules.dream2nix.nodejs-granular-v3
                    ];

                    mkDerivation = {
                      # the extension's sources, with the module `npm run wasm` would build;
                      # `vsce` runs the prepublish script, which builds that module, so it is dropped
                      src =
                        pkgs.runCommandLocal "nemo-vscode-extension-source" { nativeBuildInputs = [ config.deps.jq ]; }
                          ''
                            cp -R ${lib.cleanSource ./nemo-vscode} $out
                            chmod -R u+w $out
                            cp -R ${config.deps.nemo-wasm-web}/lib/node_modules/nemo-wasm $out/wasm
                            cat ${./LICENSE-APACHE} ${./LICENSE-MIT} > $out/LICENSE
                            jq 'del(.scripts["vscode:prepublish"])' ${./nemo-vscode/package.json} > $out/package.json
                          '';

                      nativeBuildInputs = [ config.deps.vsce ];

                      installPhase = ''
                        runHook preInstall

                        mkdir -p $out
                        cp nemo-${config.version}.vsix $out/

                        runHook postInstall
                      '';
                    };

                    deps =
                      { nixpkgs, ... }:
                      {
                        inherit (nixpkgs) jq vsce;
                        nemo-wasm-web = lib.mkDefault self.packages.${system}.nemo-wasm-web;
                      };

                    nodejs-package-lock-v3.packageLockFile = ./nemo-vscode/package-lock.json;

                    nodejs-granular-v3.buildScript = "npm run compile && vsce package";
                  }
                )
              ];
            };

            nemo-vscode-extension =
              let
                pname = "nemo";
                publisher = "knowsys";
                inherit (vscodeExtensionMeta) version;
              in
              pkgs.vscode-utils.buildVscodeExtension {
                inherit pname version;
                name = "${pname}-${version}";
                src = "${self.packages.${system}.nemo-vscode-extension-vsix}/${pname}-${version}.vsix";
                vscodeExtUniqueId = "${publisher}.${pname}";
                vscodeExtPublisher = publisher;
                vscodeExtName = pname;

                nativeBuildInputs = lib.attrValues {
                  inherit (pkgs) jq moreutils unzip;
                };

                unpackPhase = ''
                  unzip $src
                '';

                # by default, the extension starts the language server of this flake
                preInstall = ''
                  jq '.contributes.configuration.properties."nemo.server.path".default = $s' \
                    --arg s "${self.packages.${system}.nemo-language-server}/bin/nemo-language-server" \
                    package.json | sponge package.json
                '';

                meta = {
                  inherit (vscodeExtensionMeta) description;
                  homepage = vscodeExtensionMeta.repository.url;
                };
              };

            nemo-web =
              (inputs.nemo-web.packages.${system}.nemo-web.extendModules {
                modules = [
                  {
                    deps = {
                      inherit (self.packages.${system}) nemo-wasm-web nemo-wasm-bundler nemo-vscode-extension-vsix;
                    };
                  }
                ];
              }).config.public;

            inherit (inputs.nemo-doc.packages.${system}) nemo-doc;

            default = self.packages.${system}.nemo;
          };

          apps =
            let
              mkApp =
                { drv, ... }:
                {
                  type = "app";
                  program = lib.getExe drv;
                };
            in
            {
              nemo-web = mkApp {
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

              ci-checks = mkApp {
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

                    cargo test --workspace --all-targets
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
                      python3 -m pytest tests -q
                      deactivate
                      rm -rf "''${VENV}"
                    popd

                    RUSTFLAGS="" wasm-pack build --weak-refs --mode=no-install nemo-wasm
                  '';
                };
              };
            };

          checks = {
            inherit (self.packages.${system})
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
            inherit (commonArgs.env) PYO3_PYTHON;

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
                  pytest
                  wasm-bindgen
                  ;
                inherit (pkgs)
                  cargo-audit
                  cargo-license
                  cargo-tarpaulin

                  maturin
                  binaryen
                  wasm-pack

                  gnuplot
                  nodejs
                  ;
              })
            ];
          };

          formatter = pkgs.nixfmt-rfc-style;
        };
      shared = {
        overlays = {
          rust-overlay = rust-overlay.overlays.default;

          default =
            final: prev:
            let
              system = final.stdenv.hostPlatform.system;
              pkgs = self.packages."${system}";
              lib = inputs.nixpkgs."${system}";
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

          nemo-web = inputs.nemo-web.overlays.default;
        };
      };
    in
    shared
    // (lib.genAttrs [
      "apps"
      "checks"
      "devShells"
      "formatter"
      "packages"
    ] (output: forAllSystems (system: (perSystem system).${output})));
}
