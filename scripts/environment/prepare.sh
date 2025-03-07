#! /usr/bin/env bash
set -e -o verbose

rustup show # causes installation of version from rust-toolchain.toml
rustup default "$(rustup show active-toolchain | awk '{print $1;}')"
rustup run stable cargo install cargo-deb --version 1.29.2
rustup run stable cargo install cross --version 0.2.1
rustup run stable cargo install cargo-nextest --version 0.9.8

cd scripts
bundle install
cd ..

# Currently fixing this to version 0.30 since version 0.31 has introduced
# a change that means it only works with versions of node > 10.
# https://github.com/igorshubovych/markdownlint-cli/issues/258
# ubuntu 20.04 gives us version 10.19. We can revert once we update the
# ci image to install a newer version of node.
sudo npm -g install markdownlint-cli@0.30

pip3 install jsonschema==3.2.0
pip3 install remarshal==0.11.2

# Make sure our release build settings are present.
. scripts/environment/release-flags.sh
