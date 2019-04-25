extern crate pmemkv_sys;
#[macro_use]
extern crate error_chain;

pub mod kvengine;

mod errors {
    error_chain! {
        errors {
            NotFound(v: String) {
                description("NotFound"),
                display("Cannot find: {}", v),
            }
            Fail
        }

        foreign_links {
            Ffi(::std::ffi::NulError);
        }
    }
}
