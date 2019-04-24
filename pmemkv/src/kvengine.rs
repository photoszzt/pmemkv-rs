use pmemkv_sys::pmemkvapi::KVEngine as KVEngineSys;
use std::error::Error;

#[derive(Debug, Clone)]
pub struct KVEngine(*mut KVEngineSys);

impl KVEngine {
    pub fn start(engine: String, config: String) -> Result<KVEngine, Box<Error>> {
        unimplemented!();
    }

    pub fn put(&mut self, key: String, value: String) -> Result<(), Box<Error>> {
        unimplemented!();
    }

    pub fn remove(&mut self, key: String) -> Result<(), Box<Error>> {
        unimplemented!();
    }
}
