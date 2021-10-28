{
  description = "Application packaged using poetry2nix";

  inputs.flake-utils.url = "github:numtide/flake-utils";
  inputs.nixpkgs.url = "github:NixOS/nixpkgs";
  inputs.poetry2nix.url = "github:nix-community/poetry2nix";

  outputs = { self, nixpkgs, flake-utils, poetry2nix }:
    {
      # Nixpkgs overlay providing the application
      overlay = nixpkgs.lib.composeManyExtensions [
        # the poetry2nix overlay provides mkPoetryApplication and friends
        poetry2nix.overlay
        # our overlay provides chartos, and has access to poetry2nix tooling,
        # thanks to composeManyExtensions
        (final: prev: {
          chartos = prev.poetry2nix.mkPoetryApplication {
            projectDir = ./.;
          };
        })
      ];
    } // (flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
          inherit system;
          overlays = [ self.overlay ];
        };
      in
      rec {
        apps = {
          chartos = pkgs.chartos;
        };
        defaultApp = apps.chartos;

        devShell = pkgs.mkShell {
          packages = [
            (pkgs.poetry2nix.mkPoetryEnv {
              projectDir = ./.;
              editablePackageSources = {
                chartos = ./chartos;
              };
            })
          ];
        };
      }));
}
