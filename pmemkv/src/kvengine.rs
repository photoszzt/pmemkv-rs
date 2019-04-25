use crate::errors::*;
use pmemkv_sys::pmemkvapi::KVEngine as KVEngineSys;
use pmemkv_sys::pmemkvapi::*;
use std::ffi::CString;
use std::os::raw::c_void;

#[derive(Debug, Clone)]
pub struct KVEngine(*mut KVEngineSys);

impl KVEngine {
    pub fn start(
        context: *mut ::std::os::raw::c_void,
        engine: String,
        config: String,
        callback: pmemkv_sys::pmemkvapi::KVStartFailureCallback,
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

    pub fn get(
        &self,
        context: *mut c_void,
        key: String,
        cb: pmemkv_sys::pmemkvapi::KVGetCallback,
    ) -> Result<()> {
        let key_str = CString::new(key)?;
        unsafe {
            pmemkv_sys::pmemkvapi::kvengine_get(
                self.0,
                context,
                key_str.to_bytes().len() as i32,
                key_str.as_ptr(),
                cb,
            )
        };
        Ok(())
    }

    pub fn exists(&self, key: String) -> Result<()> {
        let key_str = CString::new(key.clone())?;
        let res = unsafe {
            pmemkv_sys::pmemkvapi::kvengine_exists(
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

    pub fn each(&self, context: *mut c_void, cb: KVEachCallback) {
        unsafe { kvengine_each(self.0, context, cb) }
    }

    pub fn each_above(&self, context: *mut c_void, key: String, cb: KVEachCallback) -> Result<()> {
        let key_str = CString::new(key)?;
        unsafe {
            kvengine_each_above(
                self.0,
                context,
                key_str.to_bytes().len() as i32,
                key_str.as_ptr(),
                cb,
            )
        }
        Ok(())
    }

    pub fn each_below(&self, context: *mut c_void, key: String, cb: KVEachCallback) -> Result<()> {
        let key_str = CString::new(key)?;
        unsafe {
            kvengine_each_below(
                self.0,
                context,
                key_str.to_bytes().len() as i32,
                key_str.as_ptr(),
                cb,
            )
        }
        Ok(())
    }

    pub fn each_between(
        &self,
        context: *mut c_void,
        key1: String,
        key2: String,
        cb: KVEachCallback,
    ) -> Result<()> {
        let key1_str = CString::new(key1)?;
        let key2_str = CString::new(key2)?;
        unsafe {
            kvengine_each_between(
                self.0,
                context,
                key1_str.to_bytes().len() as i32,
                key1_str.as_ptr(),
                key2_str.to_bytes().len() as i32,
                key2_str.as_ptr(),
                cb,
            )
        }
        Ok(())
    }

    pub fn count(&self) -> i64 {
        unsafe { kvengine_count(self.0) }
    }

    pub fn count_above(&self, key: String) -> Result<i64> {
        let key_str = CString::new(key)?;
        Ok(unsafe {
            kvengine_count_above(self.0, key_str.to_bytes().len() as i32, key_str.as_ptr())
        })
    }

    pub fn count_below(&self, key: String) -> Result<i64> {
        let key_str = CString::new(key)?;
        Ok(unsafe {
            kvengine_count_below(self.0, key_str.to_bytes().len() as i32, key_str.as_ptr())
        })
    }

    pub fn count_between(&self, key1: String, key2: String) -> Result<i64> {
        let key1_str = CString::new(key1)?;
        let key2_str = CString::new(key2)?;
        Ok(unsafe {
            kvengine_count_between(
                self.0,
                key1_str.to_bytes().len() as i32,
                key1_str.as_ptr(),
                key2_str.to_bytes().len() as i32,
                key2_str.as_ptr(),
            )
        })
    }

    pub fn all(&mut self, context: *mut c_void, cb: KVAllCallback) {
        unsafe { kvengine_all(self.0, context, cb) }
    }

    pub fn all_above(&mut self, context: *mut c_void, key: String, cb: KVAllCallback) -> Result<()> {
        let key_str = CString::new(key)?;
        Ok(unsafe {
            kvengine_all_above(
                self.0,
                context,
                key_str.to_bytes().len() as i32,
                key_str.as_ptr(),
                cb,
            )
        })
    }

    pub fn all_below(&mut self, context: *mut c_void, key: String, cb: KVAllCallback) -> Result<()> {
        let key_str = CString::new(key)?;
        Ok(unsafe {
            kvengine_all_below(
                self.0,
                context,
                key_str.to_bytes().len() as i32,
                key_str.as_ptr(),
                cb,
            )
        })
    }

    pub fn all_between(&mut self, context: *mut c_void, key1: String, key2: String, cb: KVAllCallback) -> Result<()> {
        let key1_str = CString::new(key1)?;
        let key2_str = CString::new(key2)?;
        Ok(unsafe {
            kvengine_all_between(
                self.0,
                context,
                key1_str.to_bytes().len() as i32,
                key1_str.as_ptr(),
                key2_str.to_bytes().len() as i32,
                key2_str.as_ptr(),
                cb,
            )
        })
    }
}
