use crate::errors::*;
use pmemkv_sys::pmemkvapi::{KVEngine as KVEngineSys, KVStartFailureCallback};
use std::ffi::CString;

#[derive(Debug, Clone)]
pub struct KVEngine(*mut KVEngineSys);

impl KVEngine {
    pub fn start(
        context: *mut ::std::os::raw::c_void,
        engine: String,
        config: String,
        callback: KVStartFailureCallback,
    ) -> Result<KVEngine> {
        use pmemkv_sys::pmemkvapi::kvengine_start;
        let engine_str = CString::new(engine)?;
        let config_str = CString::new(config)?;
        let kvengine =
            unsafe { kvengine_start(context, engine_str.as_ptr(), config_str.as_ptr(), callback) };
        Ok(KVEngine(kvengine))
    }

    pub fn stop(&mut self) {
        unsafe {
            pmemkv_sys::pmemkvapi::kvengine_stop(self.0);
        }
    }

    pub fn put(&mut self, key: String, value: String) -> Result<()> {
        let key_str = CString::new(key.clone())?;
        let val_str = CString::new(value)?;
        let res = unsafe {
            pmemkv_sys::pmemkvapi::kvengine_put(
                self.0,
                key_str.to_bytes().len() as i32,
                key_str.as_ptr(),
                val_str.to_bytes().len() as i32,
                val_str.as_ptr(),
            )
        };
        if res == 0 {
            Ok(())
        } else if res == -1 {
            Err(ErrorKind::Fail.into())
        } else {
            Err(ErrorKind::NotFound(key).into())
        }
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        let key_str = CString::new(key.clone())?;
        let res = unsafe {
            pmemkv_sys::pmemkvapi::kvengine_remove(
                self.0,
                key_str.to_bytes().len() as i32,
                key_str.as_ptr(),
            )
        };
        if res == 0 {
            Ok(())
        } else if res == -1 {
            Err(ErrorKind::Fail.into())
        } else {
            Err(ErrorKind::NotFound(key).into())
        }
    }
}
