#!/bin/bash
wget https://raw.githubusercontent.com/rust-lang/rustup.rs/master/rustup-init.sh
chmod u+x rustup-init.sh
./rustup-init.sh -y
export PATH=$HOME/.cargo/bin:$PATH
cargo build --verbose --all
cargo test --verbose --all
