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
      in rec {
        devShell = pkgs.mkShell {
          RUST_LOG = "debug";
          RUST_BACKTRACE = 1;

          shellHook = ''
            export PATH=''${HOME}/.cargo/bin''${PATH+:''${PATH}}
          '';

          buildInputs = [
            (pkgs.rust-bin.selectLatestNightlyWith (toolchain:
              toolchain.default.override {
                extensions = [ "rust-src" "miri" ];
              }))
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
