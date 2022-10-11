#!/usr/bin/env nix-shell
#!nix-shell -i zsh -p zsh

hyperfine -L mode release,release-lto,release-lto-opt2,releaserf,release-ltorf,release-lto-opt2rf,release_cg,release-lto_cg,release-lto-opt2_cg,releaserf_cg,release-ltorf_cg,release-lto-opt2rf_cg 'target/{mode}/stage2' -N -M 5000 -m 5000 -w 1000
