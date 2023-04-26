rec {
  description =
    "nemo, a datalog-based rule engine for fast and scalable analytic data processing in memory";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-22.11";

    rust-overlay.url = "github:oxalica/rust-overlay";
    rust-overlay.inputs = {
      nixpkgs.follows = "nixpkgs";
      flake-utils.follows = "utils/flake-utils";
    };

    utils.url = "github:gytis-ivaskevicius/flake-utils-plus";
  };

  outputs = inputs@{ self, utils, rust-overlay, ... }:
    utils.lib.mkFlake {
      inherit self inputs;

      channels.nixpkgs.overlaysBuilder = channels:
        [ rust-overlay.overlays.default ];

      outputsBuilder = channels:
        let
          pkgs = channels.nixpkgs;
          toolchain = (pkgs.rust-bin.selectLatestNightlyWith (toolchain:
            toolchain.default.override {
              extensions = [ "rust-src" "miri" ];
            }));
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

          packages = rec {
            nemo = platform.buildRustPackage {
              pname = "nemo";
              version = "0.1.0";
              src = ./.;

              cargoLock.lockFile = ./nix/Cargo.lock;
              postPatch = ''
                ln -s ${./nix/Cargo.lock} Cargo.lock
              '';

              meta = {
                inherit description;
                homepage = "htps://github.com/knowsys/nemo";
                license = [ pkgs.lib.licenses.asl20 pkgs.lib.licenses.mit ];
              };
            };

            default = nemo;
          };

          devShells.default = pkgs.mkShell {
            RUST_LOG = "debug";
            RUST_BACKTRACE = 1;

            shellHook = ''
              export PATH=''${HOME}/.cargo/bin''${PATH+:''${PATH}}
            '';

            buildInputs = [
              toolchain
              pkgs.rust-analyzer
              pkgs.cargo-audit
              pkgs.cargo-license
              pkgs.cargo-tarpaulin
              pkgs.valgrind
              pkgs.gnuplot
            ];
          };
        };
    };
}
