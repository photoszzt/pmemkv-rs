#!/bin/bash
curl https://sh.rustup.rs -sSf | sh

cargo build --verbose --all
cargo test --verbose --all
