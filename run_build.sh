#!/bin/bash
set -e
wget https://raw.githubusercontent.com/rust-lang/rustup.rs/master/rustup-init.sh
chmod u+x rustup-init.sh
./rustup-init.sh -y
export PATH=$HOME/.cargo/bin:$PATH
git clone https://github.com/pmem/pmemkv
cd pmemkv
mkdir bin
cd bin
cmake .. -DCMAKE_BUILD_TYPE=Release \
    -DTBB_DIR=/opt/tbb/cmake \
    -DCMAKE_INSTALL_PREFIX=/usr/local
make
echo pass | sudo -S make install
cd /pmemkv-rs
cargo build --verbose --all
cargo test --verbose --all
