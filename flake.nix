{
  description = "stage2, the next stage of Datalog reasoning";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-21.05";
    rust-overlay.url = "github:oxalica/rust-overlay";
    flake-utils.url = "github:numtide/flake-utils";
    flake-compat = {
      url = "github:edolstra/flake-compat";
      flake = false;
    };
    gitignoresrc = {
      url = "github:hercules-ci/gitignore.nix";
      flake = false;
    };
  };

  outputs = { self, nixpkgs, flake-utils, flake-compat, gitignoresrc, rust-overlay, ... }@inputs:
    { } // (flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
          inherit system;
          overlays = [ (import rust-overlay) ];
        };
      in
      rec {
        devShell =
          pkgs.mkShell {
            RUST_LOG = "debug";
            RUST_BACKTRACE = 1;

            buildInputs = [
              pkgs.rust-bin.stable.latest.default
              pkgs.rust-analyzer
              pkgs.cargo-audit
              pkgs.cargo-license
              pkgs.valgrind
              pkgs.gnuplot
            ];
          };
      }
    ));
}
