extern crate pmemkv_sys;
#[macro_use]
extern crate error_chain;

pub mod kvengine;

pub mod errors {
    error_chain! {
        errors {
            #[derive(partial_eq)]
            NotFound(v: String) {
                description("NotFound"),
                display("Cannot find: {}", v),
            }
            #[derive(partial_eq)]
            Fail
        }

        foreign_links {
            Ffi(::std::ffi::NulError);
        }
    }
}
