{
  description = "nemo, the next stage of Datalog reasoning";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-22.05";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs = {
        nixpkgs.follows = "nixpkgs";
        flake-utils.follows = "flake-utils";
      };
    };
    flake-utils.url = "github:numtide/flake-utils";
    gitignoresrc = {
      url = "github:hercules-ci/gitignore.nix";
      flake = false;
    };
  };

  outputs = { self, nixpkgs, flake-utils, rust-overlay, ... }@inputs:
    { } // (flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
          inherit system;
          overlays = [ (import rust-overlay) ];
        };
        toolchain = (pkgs.rust-bin.selectLatestNightlyWith (toolchain:
          toolchain.default.override { extensions = [ "rust-src" "miri" ]; }));
        platform = pkgs.makeRustPlatform {
          cargo = toolchain;
          rustc = toolchain;
        };
      in rec {

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
              description = "nemo, the next stage of Datalog reasoning";
              homepage = "htps://github.com/knowsys/nemo";
              license = [ pkgs.lib.licenses.asl20 ];
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
      }));
}
