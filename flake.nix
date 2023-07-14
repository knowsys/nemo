rec {
  description = "nemo, a datalog-based rule engine for fast and scalable analytic data processing in memory";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-23.05";

    rust-overlay.url = "github:oxalica/rust-overlay";
    rust-overlay.inputs = {
      nixpkgs.follows = "nixpkgs";
      flake-utils.follows = "utils/flake-utils";
    };

    utils.url = "github:gytis-ivaskevicius/flake-utils-plus";
  };

  outputs = inputs @ {
    self,
    utils,
    rust-overlay,
    ...
  }:
    utils.lib.mkFlake {
      inherit self inputs;

      channels.nixpkgs.overlaysBuilder = channels: [rust-overlay.overlays.default];

      overlays.default = final: prev: let
        pkgs = self.packages."${final.system}";
        lib = self.channels.nixpkgs."${final.system}";
      in {
        inherit (pkgs) nemo nemo-python nemo-wasm wasm-bindgen-cli;

        nodePackages = lib.makeExtensible (lib.extends pkgs.nodePackages prev.nodePackages);
      };

      outputsBuilder = channels: let
        pkgs = channels.nixpkgs;
        toolchain = pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;
        platform = pkgs.makeRustPlatform {
          cargo = toolchain;
          rustc = toolchain;
        };
        defaultBuildInputs = [pkgs.openssl pkgs.openssl.dev];
        defaultNativeBuildInputs = [
          toolchain
          pkgs.pkg-config
        ];
      in rec {
        packages = let
          cargoMeta = (builtins.fromTOML (builtins.readFile ./Cargo.toml)).workspace.package;
          inherit (cargoMeta) version;
          meta = {
            inherit description;
            inherit (cargoMeta) homepage;
            license = [pkgs.lib.licenses.asl20 pkgs.lib.licenses.mit];
          };

          buildWasm = target: {
            pname,
            src,
            version,
            meta,
            ...
          } @ args:
            pkgs.stdenv.mkDerivation rec {
              inherit (args) version meta pname src;

              cargoDeps =
                platform.importCargoLock {lockFile = ./Cargo.lock;};

              nativeBuildInputs =
                defaultNativeBuildInputs
                ++ (with platform; [
                  cargoSetupHook
                  pkgs.wasm-pack
                  pkgs.nodejs
                  self.packages."${pkgs.system}".wasm-bindgen-cli
                ]);
              buildInputs = defaultBuildInputs ++ [pkgs.nodejs];

              buildPhase = ''
                runHook preBuild

                mkdir -p $out/lib/node_modules/nemo-wasm
                mkdir .cache
                mkdir target
                export CARGO_HOME=$TMPDIR/.cargo
                export CARGO_TARGET_DIR=$TMPDIR/target
                export XDG_CACHE_HOME=$TMPDIR/.cache

                cd $src
                wasm-pack build --target ${target} --weak-refs --mode=no-install --out-dir=$out/lib/node_modules/${pname} nemo-wasm

                runHook postBuild
              '';
            };
        in rec {
          nemo = platform.buildRustPackage {
            pname = "nemo";
            src = ./.;
            meta = meta // {mainProgram = "nmo";};
            inherit version;

            cargoLock.lockFile = ./Cargo.lock;

            buildInputs = defaultBuildInputs;
            nativeBuildInputs =
              defaultNativeBuildInputs
              ++ (with platform; [
                cargoBuildHook
                cargoCheckHook
              ]);
            buildAndTestSubdir = "nemo-cli";
          };

          nemo-python = pkgs.python3Packages.buildPythonPackage {
            pname = "nemo-python";
            src = ./.;
            inherit version meta;

            cargoDeps = platform.importCargoLock {lockFile = ./Cargo.lock;};

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
          nemo-wasm = nemo-wasm-bundler;

          python3 = pkgs.python3.withPackages (ps: [nemo-python]);
          python = python3;

          nodejs = pkgs.writeShellScriptBin "node" ''
            NODE_PATH=${nemo-wasm-node}/lib/node_modules''${NODE_PATH:+":$NODE_PATH"} ${pkgs.nodejs}/bin/node $@
          '';

          # wasm-pack expects wasm-bindgen 0.2.87,
          # but nixpkgs currently only has 0.2.84
          wasm-bindgen-cli = pkgs.wasm-bindgen-cli.overrideAttrs (old: rec {
            version = "0.2.87";
            src = pkgs.fetchCrate {
              inherit (old) pname;
              inherit version;
              sha256 = "sha256-0u9bl+FkXEK2b54n7/l9JOCtKo+pb42GF9E1EnAUQa0=";
            };

            cargoDeps = platform.fetchCargoTarball {
              inherit src;
              hash = "sha256-GncJhqH/ZYFu/NPRpkpcHJiSq6lC5WQXo/Fmy3iyviA=";
            };

            doCheck = false;
          });

          default = nemo;
        };

        checks = let
          runCargo' = name: env: buildCommand:
            pkgs.stdenv.mkDerivation ({
                preferLocalBuild = true;
                allowSubstitutes = false;

                RUSTFLAGS = "-Dwarnings";
                RUSTDOCFLAGS = "-Dwarnings";

                src = ./.;
                cargoDeps = platform.importCargoLock {lockFile = ./Cargo.lock;};

                buildInputs = defaultBuildInputs;
                nativeBuildInputs = defaultNativeBuildInputs ++ (with platform; [cargoSetupHook pkgs.python3]);

                inherit name;

                buildPhase = ''
                  runHook preBuild
                  mkdir $out
                  ${buildCommand}
                  runHook postBuild
                '';
              }
              // env);
          runCargo = name: runCargo' name {};
        in rec {
          inherit (packages) nemo nemo-python nemo-wasm nemo-wasm-node;
          devShell = devShells.default;

          clippy = runCargo "nemo-check-clippy" ''
            cargo clippy --all-targets
          '';

          doc = runCargo "nemo-check-docs" ''
            cargo doc --workspace
          '';

          test = runCargo "nemo-check-tests" ''
            cargo test
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

          buildInputs = let
            ifNotOn = systems:
              pkgs.lib.optionals (!builtins.elem pkgs.system systems);
          in
            pkgs.lib.concatLists [
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
                self.packages."${pkgs.system}".wasm-bindgen-cli
                pkgs.nodejs
              ]
              # valgrind is linux-only
              (ifNotOn ["aarch64-darwin" "x86_64-darwin"] [pkgs.valgrind])
            ];
        };

        formatter = channels.nixpkgs.alejandra;
      };
    };
}
# Local Variables:
# apheleia-formatter: alejandra
# End:
