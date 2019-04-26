use crate::errors::*;
use pmemkv_sys::pmemkvapi::KVEngine as KVEngineSys;
use pmemkv_sys::pmemkvapi::*;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_void};

#[derive(Debug, Clone)]
pub struct KVEngine(*mut KVEngineSys);

impl Drop for KVEngine {
    fn drop(&mut self) {
        unsafe {
            kvengine_stop(self.0);
        }
    }
}

extern "C" fn cb_wrapper<F>(closure: *mut c_void, bytes: c_int, v: *const c_char)
where
    F: Fn(c_int, *const c_char),
{
    let opt_closure: &mut Box<Fn(c_int, *const c_char)> = unsafe { ::std::mem::transmute(closure) };
    opt_closure(bytes, v);
}

extern "C" fn cb_string_wrapper<F>(closure: *mut c_void, _b: c_int, v: *const c_char)
where
    F: Fn(String),
{
    let opt_closure: &mut Box<Fn(String)> = unsafe { ::std::mem::transmute(closure) };
    let s = unsafe {
        CStr::from_ptr(v).to_string_lossy().into_owned()
    };
    opt_closure(s)
}

extern "C" fn cb_each_wrapper<F>(
    closure: *mut c_void,
    kb: c_int,
    k: *const c_char,
    vb: c_int,
    v: *const c_char,
) where
    F: Fn(c_int, *const c_char, c_int, *const c_char),
{
    let opt_closure: &mut Box<Fn(c_int, *const c_char, c_int, *const c_char)> =
        unsafe { ::std::mem::transmute(closure) };
    opt_closure(kb, k, vb, v);
}

extern "C" fn cb_each_string_wrapper<F>(
    closure: *mut c_void,
    _kb: c_int,
    k: *const c_char,
    _vb: c_int,
    v: *const c_char,
) where
    F: Fn(String, String),
{
    let opt_closure: &mut Box<Fn(String, String)> = unsafe { ::std::mem::transmute(closure) };
    let (k_s, v_s) = unsafe {
        (
            CStr::from_ptr(k).to_string_lossy().into_owned(),
            CStr::from_ptr(v).to_string_lossy().into_owned(),
        )
    };
    opt_closure(k_s, v_s)
}

impl KVEngine {
    pub fn start(
        context: *mut ::std::os::raw::c_void,
        engine: String,
        config: String,
        callback: KVStartFailureCallback,
    ) -> Result<KVEngine> {
        let engine_str = CString::new(engine)?;
        let config_str = CString::new(config)?;
        let kvengine =
            unsafe { kvengine_start(context, engine_str.as_ptr(), config_str.as_ptr(), callback) };
        if kvengine == ::std::ptr::null_mut() {
            Err(ErrorKind::Fail.into())
        } else {
            Ok(KVEngine(kvengine))
        }
    }

    pub fn put(&mut self, key: String, value: String) -> Result<()> {
        let key_str = CString::new(key.clone())?;
        let val_str = CString::new(value)?;
        let res = unsafe {
            kvengine_put(
                self.0,
                key_str.to_bytes().len() as i32,
                key_str.as_ptr(),
                val_str.to_bytes().len() as i32,
                val_str.as_ptr(),
            )
        };
        if res == 1 {
            Ok(())
        } else if res == -1 {
            Err(ErrorKind::Fail.into())
        } else {
            Err(ErrorKind::NotFound(key).into())
        }
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        let key_str = CString::new(key.clone())?;
        let res =
            unsafe { kvengine_remove(self.0, key_str.to_bytes().len() as i32, key_str.as_ptr()) };
        if res == 1 {
            Ok(())
        } else if res == -1 {
            Err(ErrorKind::Fail.into())
        } else {
            Err(ErrorKind::NotFound(key).into())
        }
    }

    pub fn get<F>(&self, key: String, callback: Option<F>) -> Result<()>
    where
        F: Fn(c_int, *const c_char),
    {
        let key_str = CString::new(key)?;
        match callback {
            Some(f) => unsafe {
                let cb: Box<Box<Fn(c_int, *const c_char) -> ()>> = Box::new(Box::new(f));
                kvengine_get(
                    self.0,
                    Box::into_raw(cb) as *mut _,
                    key_str.to_bytes().len() as i32,
                    key_str.as_ptr(),
                    Some(cb_wrapper::<F>),
                )
            },
            None => unsafe {
                kvengine_get(
                    self.0,
                    ::std::ptr::null_mut(),
                    key_str.to_bytes().len() as i32,
                    key_str.as_ptr(),
                    None,
                )
            },
        }
        Ok(())
    }

    pub fn get_string<F>(&self, key: String, callback: Option<F>) -> Result<()>
    where
        F: Fn(String),
    {
        let key_str = CString::new(key)?;
        match callback {
            Some(f) => unsafe {
                let cb: Box<Box<Fn(String) -> ()>> = Box::new(Box::new(f));
                kvengine_get(
                    self.0,
                    Box::into_raw(cb) as *mut _,
                    key_str.to_bytes().len() as i32,
                    key_str.as_ptr(),
                    Some(cb_string_wrapper::<F>),
                )
            },
            None => unsafe {
                kvengine_get(
                    self.0,
                    ::std::ptr::null_mut(),
                    key_str.to_bytes().len() as i32,
                    key_str.as_ptr(),
                    None,
                )
            },
        }
        Ok(())
    }

    pub fn get_copy(&self, key: String, max_value_bytes: i32) -> Result<String> {
        let key_str = CString::new(key.clone())?;
        let val_vec = Vec::with_capacity(max_value_bytes as usize);
        let res = unsafe {
            kvengine_get_copy(
                self.0,
                key_str.to_bytes().len() as i32,
                key_str.as_ptr(),
                max_value_bytes,
                val_vec.as_ptr() as *mut _,
            )
        };
        if res == 1 {
            Ok(unsafe {
                CStr::from_ptr(val_vec.as_ptr())
                    .to_string_lossy()
                    .into_owned()
            })
        } else if res == -1 {
            Err(ErrorKind::Fail.into())
        } else {
            Err(ErrorKind::NotFound(key).into())
        }
    }

    pub fn exists(&self, key: String) -> Result<()> {
        let key_str = CString::new(key.clone())?;
        let res =
            unsafe { kvengine_exists(self.0, key_str.to_bytes().len() as i32, key_str.as_ptr()) };
        if res == 1 {
            Ok(())
        } else if res == -1 {
            Err(ErrorKind::Fail.into())
        } else {
            Err(ErrorKind::NotFound(key).into())
        }
    }

    pub fn each<F>(&self, callback: Option<F>)
    where
        F: Fn(c_int, *const c_char, c_int, *const c_char),
    {
        match callback {
            Some(f) => unsafe {
                let cb: Box<Box<Fn(c_int, *const c_char, c_int, *const c_char) -> ()>> =
                    Box::new(Box::new(f));
                kvengine_each(
                    self.0,
                    Box::into_raw(cb) as *mut _,
                    Some(cb_each_wrapper::<F>),
                )
            },
            None => unsafe { kvengine_each(self.0, ::std::ptr::null_mut(), None) },
        }
    }

    pub fn each_string<F>(&self, callback: Option<F>)
    where
        F: Fn(String, String),
    {
        match callback {
            Some(f) => unsafe {
                let cb: Box<Box<Fn(String, String) -> ()>> = Box::new(Box::new(f));
                kvengine_each(
                    self.0,
                    Box::into_raw(cb) as *mut _,
                    Some(cb_each_string_wrapper::<F>),
                )
            },
            None => unsafe { kvengine_each(self.0, ::std::ptr::null_mut(), None) },
        }
    }

    pub fn each_above<F>(&self, key: String, callback: Option<F>) -> Result<()>
    where
        F: Fn(c_int, *const c_char, c_int, *const c_char),
    {
        let key_str = CString::new(key)?;
        match callback {
            Some(f) => unsafe {
                let cb: Box<Box<Fn(c_int, *const c_char, c_int, *const c_char) -> ()>> =
                    Box::new(Box::new(f));
                kvengine_each_above(
                    self.0,
                    Box::into_raw(cb) as *mut _,
                    key_str.to_bytes().len() as i32,
                    key_str.as_ptr(),
                    Some(cb_each_wrapper::<F>),
                )
            },
            None => unsafe {
                kvengine_each_above(
                    self.0,
                    ::std::ptr::null_mut(),
                    key_str.to_bytes().len() as i32,
                    key_str.as_ptr(),
                    None,
                )
            },
        }
        Ok(())
    }

    pub fn each_above_string<F>(&self, key: String, callback: Option<F>) -> Result<()>
    where
        F: Fn(String, String),
    {
        let key_str = CString::new(key)?;
        match callback {
            Some(f) => unsafe {
                let cb: Box<Box<Fn(String, String) -> ()>> = Box::new(Box::new(f));
                kvengine_each_above(
                    self.0,
                    Box::into_raw(cb) as *mut _,
                    key_str.to_bytes().len() as i32,
                    key_str.as_ptr(),
                    Some(cb_each_string_wrapper::<F>),
                )
            },
            None => unsafe {
                kvengine_each_above(
                    self.0,
                    ::std::ptr::null_mut(),
                    key_str.to_bytes().len() as i32,
                    key_str.as_ptr(),
                    None,
                )
            },
        }
        Ok(())
    }

    pub fn each_below<F>(&self, key: String, callback: Option<F>) -> Result<()>
    where
        F: Fn(c_int, *const c_char, c_int, *const c_char),
    {
        let key_str = CString::new(key)?;
        match callback {
            Some(f) => unsafe {
                let cb: Box<Box<Fn(c_int, *const c_char, c_int, *const c_char) -> ()>> =
                    Box::new(Box::new(f));
                kvengine_each_below(
                    self.0,
                    Box::into_raw(cb) as *mut _,
                    key_str.to_bytes().len() as i32,
                    key_str.as_ptr(),
                    Some(cb_each_wrapper::<F>),
                )
            },
            None => unsafe {
                kvengine_each_below(
                    self.0,
                    ::std::ptr::null_mut(),
                    key_str.to_bytes().len() as i32,
                    key_str.as_ptr(),
                    None,
                )
            },
        }
        Ok(())
    }

    pub fn each_below_string<F>(&self, key: String, callback: Option<F>) -> Result<()>
    where
        F: Fn(String, String),
    {
        let key_str = CString::new(key)?;
        match callback {
            Some(f) => unsafe {
                let cb: Box<Box<Fn(String, String) -> ()>> = Box::new(Box::new(f));
                kvengine_each_below(
                    self.0,
                    Box::into_raw(cb) as *mut _,
                    key_str.to_bytes().len() as i32,
                    key_str.as_ptr(),
                    Some(cb_each_string_wrapper::<F>),
                )
            },
            None => unsafe {
                kvengine_each_below(
                    self.0,
                    ::std::ptr::null_mut(),
                    key_str.to_bytes().len() as i32,
                    key_str.as_ptr(),
                    None,
                )
            },
        }
        Ok(())
    }

    pub fn each_between<F>(&self, key1: String, key2: String, callback: Option<F>) -> Result<()>
    where
        F: Fn(c_int, *const c_char, c_int, *const c_char),
    {
        let key1_str = CString::new(key1)?;
        let key2_str = CString::new(key2)?;
        match callback {
            Some(f) => unsafe {
                let cb: Box<Box<Fn(c_int, *const c_char, c_int, *const c_char) -> ()>> =
                    Box::new(Box::new(f));
                kvengine_each_between(
                    self.0,
                    Box::into_raw(cb) as *mut _,
                    key1_str.to_bytes().len() as i32,
                    key1_str.as_ptr(),
                    key2_str.to_bytes().len() as i32,
                    key2_str.as_ptr(),
                    Some(cb_each_wrapper::<F>),
                )
            },
            None => unsafe {
                kvengine_each_between(
                    self.0,
                    ::std::ptr::null_mut(),
                    key1_str.to_bytes().len() as i32,
                    key1_str.as_ptr(),
                    key2_str.to_bytes().len() as i32,
                    key2_str.as_ptr(),
                    None,
                )
            },
        }
        Ok(())
    }

    pub fn each_between_string<F>(
        &self,
        key1: String,
        key2: String,
        callback: Option<F>,
    ) -> Result<()>
    where
        F: Fn(String, String),
    {
        let key1_str = CString::new(key1)?;
        let key2_str = CString::new(key2)?;
        match callback {
            Some(f) => unsafe {
                let cb: Box<Box<Fn(String, String) -> ()>> = Box::new(Box::new(f));
                kvengine_each_between(
                    self.0,
                    Box::into_raw(cb) as *mut _,
                    key1_str.to_bytes().len() as i32,
                    key1_str.as_ptr(),
                    key2_str.to_bytes().len() as i32,
                    key2_str.as_ptr(),
                    Some(cb_each_string_wrapper::<F>),
                )
            },
            None => unsafe {
                kvengine_each_between(
                    self.0,
                    ::std::ptr::null_mut(),
                    key1_str.to_bytes().len() as i32,
                    key1_str.as_ptr(),
                    key2_str.to_bytes().len() as i32,
                    key2_str.as_ptr(),
                    None,
                )
            },
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

    pub fn all<F>(&mut self, callback: Option<F>)
    where
        F: Fn(c_int, *const c_char),
    {
        match callback {
            Some(f) => unsafe {
                let cb: Box<Box<Fn(c_int, *const c_char) -> ()>> = Box::new(Box::new(f));
                kvengine_all(self.0, Box::into_raw(cb) as *mut _, Some(cb_wrapper::<F>))
            },
            None => unsafe { kvengine_all(self.0, ::std::ptr::null_mut(), None) },
        }
    }

    pub fn all_string<F>(&mut self, callback: Option<F>)
    where
        F: Fn(String),
    {
        match callback {
            Some(f) => unsafe {
                let cb: Box<Box<Fn(String) -> ()>> = Box::new(Box::new(f));
                kvengine_all(
                    self.0,
                    Box::into_raw(cb) as *mut _,
                    Some(cb_string_wrapper::<F>),
                )
            },
            None => unsafe { kvengine_all(self.0, ::std::ptr::null_mut(), None) },
        }
    }

    pub fn all_above<F>(&mut self, key: String, callback: Option<F>) -> Result<()>
    where
        F: Fn(c_int, *const c_char),
    {
        let key_str = CString::new(key)?;
        match callback {
            Some(f) => Ok(unsafe {
                let cb: Box<Box<Fn(c_int, *const c_char) -> ()>> = Box::new(Box::new(f));
                kvengine_all_above(
                    self.0,
                    Box::into_raw(cb) as *mut _,
                    key_str.to_bytes().len() as i32,
                    key_str.as_ptr(),
                    Some(cb_wrapper::<F>),
                )
            }),
            None => Ok(unsafe {
                kvengine_all_above(
                    self.0,
                    ::std::ptr::null_mut(),
                    key_str.to_bytes().len() as i32,
                    key_str.as_ptr(),
                    None,
                )
            }),
        }
    }

    pub fn all_above_string<F>(&mut self, key: String, callback: Option<F>) -> Result<()>
    where
        F: Fn(String),
    {
        let key_str = CString::new(key)?;
        match callback {
            Some(f) => Ok(unsafe {
                let cb: Box<Box<Fn(String) -> ()>> = Box::new(Box::new(f));
                kvengine_all_above(
                    self.0,
                    Box::into_raw(cb) as *mut _,
                    key_str.to_bytes().len() as i32,
                    key_str.as_ptr(),
                    Some(cb_string_wrapper::<F>),
                )
            }),
            None => Ok(unsafe {
                kvengine_all_above(
                    self.0,
                    ::std::ptr::null_mut(),
                    key_str.to_bytes().len() as i32,
                    key_str.as_ptr(),
                    None,
                )
            }),
        }
    }

    pub fn all_below<F>(&mut self, key: String, callback: Option<F>) -> Result<()>
    where
        F: Fn(c_int, *const c_char),
    {
        let key_str = CString::new(key)?;
        match callback {
            Some(f) => Ok(unsafe {
                let cb: Box<Box<Fn(c_int, *const c_char) -> ()>> = Box::new(Box::new(f));
                kvengine_all_below(
                    self.0,
                    Box::into_raw(cb) as *mut _,
                    key_str.to_bytes().len() as i32,
                    key_str.as_ptr(),
                    Some(cb_wrapper::<F>),
                )
            }),
            None => Ok(unsafe {
                kvengine_all_below(
                    self.0,
                    ::std::ptr::null_mut(),
                    key_str.to_bytes().len() as i32,
                    key_str.as_ptr(),
                    None,
                )
            }),
        }
    }

    pub fn all_below_string<F>(&mut self, key: String, callback: Option<F>) -> Result<()>
    where
        F: Fn(String),
    {
        let key_str = CString::new(key)?;
        match callback {
            Some(f) => Ok(unsafe {
                let cb: Box<Box<Fn(String) -> ()>> = Box::new(Box::new(f));
                kvengine_all_below(
                    self.0,
                    Box::into_raw(cb) as *mut _,
                    key_str.to_bytes().len() as i32,
                    key_str.as_ptr(),
                    Some(cb_string_wrapper::<F>),
                )
            }),
            None => Ok(unsafe {
                kvengine_all_below(
                    self.0,
                    ::std::ptr::null_mut(),
                    key_str.to_bytes().len() as i32,
                    key_str.as_ptr(),
                    None,
                )
            }),
        }
    }

    pub fn all_between<F>(&mut self, key1: String, key2: String, callback: Option<F>) -> Result<()>
    where
        F: Fn(c_int, *const c_char),
    {
        let key1_str = CString::new(key1)?;
        let key2_str = CString::new(key2)?;
        match callback {
            Some(f) => Ok(unsafe {
                let cb: Box<Box<Fn(c_int, *const c_char) -> ()>> = Box::new(Box::new(f));
                kvengine_all_between(
                    self.0,
                    Box::into_raw(cb) as *mut _,
                    key1_str.to_bytes().len() as i32,
                    key1_str.as_ptr(),
                    key2_str.to_bytes().len() as i32,
                    key2_str.as_ptr(),
                    Some(cb_wrapper::<F>),
                )
            }),
            None => Ok(unsafe {
                kvengine_all_between(
                    self.0,
                    ::std::ptr::null_mut(),
                    key1_str.to_bytes().len() as i32,
                    key1_str.as_ptr(),
                    key2_str.to_bytes().len() as i32,
                    key2_str.as_ptr(),
                    None,
                )
            }),
        }
    }

    pub fn all_between_string<F>(
        &mut self,
        key1: String,
        key2: String,
        callback: Option<F>,
    ) -> Result<()>
    where
        F: Fn(String),
    {
        let key1_str = CString::new(key1)?;
        let key2_str = CString::new(key2)?;
        match callback {
            Some(f) => Ok(unsafe {
                let cb: Box<Box<Fn(String) -> ()>> = Box::new(Box::new(f));
                kvengine_all_between(
                    self.0,
                    Box::into_raw(cb) as *mut _,
                    key1_str.to_bytes().len() as i32,
                    key1_str.as_ptr(),
                    key2_str.to_bytes().len() as i32,
                    key2_str.as_ptr(),
                    Some(cb_string_wrapper::<F>),
                )
            }),
            None => Ok(unsafe {
                kvengine_all_between(
                    self.0,
                    ::std::ptr::null_mut(),
                    key1_str.to_bytes().len() as i32,
                    key1_str.as_ptr(),
                    key2_str.to_bytes().len() as i32,
                    key2_str.as_ptr(),
                    None,
                )
            }),
        }
    }
}
