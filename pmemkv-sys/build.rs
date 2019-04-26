use bindgen;
use std::process;

fn main() {
    linking_info();
    build_pmemkv_bindings();
}

fn linking_info() {
    println!("cargo:rustc-link-lib=pmemkv");
    println!("cargo:rustc-link-lib=tbb");
}

const WHITELIST_FUNCTION: &'static [&'static str] = &["kvengine_.*"];

const WHITELIST_TYPES: &'static [&'static str] = &["KVEngine", "KVStatus"];

fn build_pmemkv_bindings() {
    let mut bindings = bindgen::Builder::default()
        .header("libpmemkv.h")
        .clang_arg("-I")
        .clang_arg(concat!(env!("CARGO_MANIFEST_DIR"), "/include"));

    for func in WHITELIST_FUNCTION {
        bindings = bindings.whitelist_function(func);
    }

    for ty in WHITELIST_TYPES {
        bindings = bindings.whitelist_type(ty);
    }

    bindings = bindings
        .derive_debug(true)
        .impl_debug(true)
        .derive_default(true)
        .derive_partialeq(true)
        .impl_partialeq(true)
        .derive_eq(true)
        .derive_partialord(true)
        .derive_ord(true)
        .derive_hash(true)
        .rustfmt_bindings(true);

    let builder = bindings
        .generate()
        .expect("Should generate PMEMKV API bindings OK");

    builder
        .write_to_file("src/pmemkvapi.rs")
        .expect("Couldn't write pmemkv bindings!");
    let have_working_rustfmt = process::Command::new("rustup")
        .args(&["run", "nightly", "rustfmt", "--version"])
        .stdout(process::Stdio::null())
        .stderr(process::Stdio::null())
        .status()
        .ok()
        .map_or(false, |status| status.success());

    if !have_working_rustfmt {
        println!(
            "
        The latest `rustfmt` is required to format the generated bindings. Install
            `rustfmt` with:
            $ rustup component add rustfmt-preview
            $ rustup update
            "
        );
    }
}
