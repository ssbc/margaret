# SPDX-FileCopyrightText: 2024 The margaret Authors
#
# SPDX-License-Identifier: MIT
{
  description = "margaret - append-only log toolkit for Go";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = {
    nixpkgs,
    flake-utils,
    ...
  }:
    flake-utils.lib.eachDefaultSystem (
      system: let
        pkgs = import nixpkgs {inherit system;};
        go = pkgs.go_1_25;
      in {
        devShells.default = pkgs.mkShell {
          packages = [
            go
            pkgs.gopls
            pkgs.gotools
            pkgs.go-tools
            pkgs.delve
          ];

          shellHook = ''
            export GOPATH="$HOME/go"
            export PATH="$GOPATH/bin:$PATH"
          '';
        };
      }
    );
}
