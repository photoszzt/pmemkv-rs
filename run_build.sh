#!/bin/bash
set -e
echo pass | sudo -S apt install libtbb-dev
wget https://raw.githubusercontent.com/rust-lang/rustup.rs/master/rustup-init.sh
chmod u+x rustup-init.sh
./rustup-init.sh -y
export PATH=$HOME/.cargo/bin:$PATH
git clone https://github.com/pmem/pmemkv pmemkv_src
cd pmemkv_src
mkdir bin
cd bin
cmake .. -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX=/usr/local
make
echo pass | sudo -S cp libpmemkv.so /usr/local/lib/
echo pass | sudo -S cp ../src/pmemkv.h /usr/local/include/libpmemkv.h
cd /pmemkv-rs
cargo build --verbose --all
cargo test --verbose --all
