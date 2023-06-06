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
      in {
        inherit (pkgs) nemo nemo-python nemo-wasm wasm-bindgen-cli;

        nodePackages.nemo-wasm = pkgs.nemo-wasm-node;
      };

      outputsBuilder = channels: let
        pkgs = channels.nixpkgs;
        toolchain = pkgs.rust-bin.selectLatestNightlyWith (toolchain:
          toolchain.default.override {
            extensions = ["rust-src" "miri"];
            targets = ["wasm32-unknown-unknown"];
          });
        platform = pkgs.makeRustPlatform {
          cargo = toolchain;
          rustc = toolchain;
        };
      in rec {
        apps = rec {
          nemo = utils.lib.mkApp {
            drv = packages.nemo;
            exePath = "/bin/nmo";
          };

          default = nemo;
        };

        packages = let
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

              nativeBuildInputs = with platform; [
                cargoSetupHook
                pkgs.wasm-pack
                pkgs.nodejs
                toolchain
                self.packages."${pkgs.system}".wasm-bindgen-cli
              ];
              buildInputs = [pkgs.nodejs];

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
            version = "0.1.1-dev";
            src = ./.;

            cargoLock.lockFile = ./Cargo.lock;

            meta = {
              inherit description;
              homepage = "htps://github.com/knowsys/nemo";
              license = [pkgs.lib.licenses.asl20 pkgs.lib.licenses.mit];
            };

            nativeBuildInputs = with platform; [
              cargoBuildHook
              cargoCheckHook
            ];
            buildAndTestSubdir = "nemo-cli";
          };

          nemo-python = pkgs.python3Packages.buildPythonPackage {
            pname = "nemo-python";
            src = ./.;
            inherit (nemo) version meta;

            cargoDeps = platform.importCargoLock {lockFile = ./Cargo.lock;};

            nativeBuildInputs = with platform; [
              cargoSetupHook
              maturinBuildHook
            ];
            buildAndTestSubdir = "nemo-python";

            doCheck = false; # no python tests yet
          };

          nemo-wasm-node = buildWasm "nodejs" {
            pname = "nemo-wasm";
            src = ./.;
            inherit (nemo) version meta;
          };
          nemo-wasm-bundler = buildWasm "bundler" {
            pname = "nemo-wasm";
            src = ./.;
            inherit (nemo) version meta;
          };
          nemo-wasm = nemo-wasm-bundler;

          python3 = pkgs.python3.withPackages (ps: [nemo-python]);
          python = python3;

          nodejs = pkgs.writeShellScriptBin "node" ''
            NODE_PATH=${nemo-wasm-node}/lib/node_modules''${NODE_PATH:+":$NODE_PATH"} ${pkgs.nodejs}/bin/node $@
          '';

          # wasm-pack expects wasm-bindgen 0.2.86,
          # but nixpkgs currently only has 0.2.84
          wasm-bindgen-cli = pkgs.wasm-bindgen-cli.overrideAttrs (old: rec {
            version = "0.2.86";
            src = pkgs.fetchCrate {
              inherit (old) pname;
              inherit version;
              sha256 = "sha256-56EOiLbdgAcoTrkyvB3t9TjtLaRvGxFUXx4haLwE2QY=";
            };

            cargoDeps = platform.fetchCargoTarball {
              inherit src;
              hash = "sha256-zQtr47XIb3G43j2IbQR+mHVnJbSriEhwOBQ7HA+YdKE=";
            };

            doCheck = false;
          });

          default = nemo;
        };

        devShells.default = pkgs.mkShell {
          NMO_LOG = "debug";
          RUST_BACKTRACE = 1;

          shellHook = ''
            export PATH=''${HOME}/.cargo/bin''${PATH+:''${PATH}}
          '';

          buildInputs = let
            ifNotOn = systems:
              pkgs.lib.optionals (!builtins.elem pkgs.system systems);
          in
            [
              toolchain
              pkgs.rust-analyzer
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
            ++ (ifNotOn ["aarch64-darwin" "x86_64-darwin"] [pkgs.valgrind]);
        };

        formatter = channels.nixpkgs.alejandra;
      };
    };
}
# Local Variables:
# apheleia-formatter: alejandra
# End:
