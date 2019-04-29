use crate::errors::*;
use pmemkv_sys::KVEngine as KVEngineSys;
use pmemkv_sys::*;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_void};
use std::slice;

#[derive(Debug, Clone)]
pub struct KVEngine(*mut KVEngineSys);

impl Drop for KVEngine {
    fn drop(&mut self) {
        unsafe {
            kvengine_stop(self.0);
        }
    }
}

type EachFn = Fn(&[c_char], &[c_char]);
type StartFn = Fn(*const c_char, *const c_char, *const c_char);
type StartStringFn = Fn(&str, &str, &str);

extern "C" fn cb_wrapper<F>(closure: *mut c_void, bytes: c_int, v: *const c_char)
where
    F: Fn(&[c_char]),
{
    let opt_closure: &&Fn(&[c_char]) =
        unsafe { &*(closure as *const &dyn std::ops::Fn(&[c_char])) };
    let slice = unsafe { slice::from_raw_parts(v, bytes as usize) };
    opt_closure(slice);
}

extern "C" fn cb_string_wrapper<F>(closure: *mut c_void, _b: c_int, v: *const c_char)
where
    F: Fn(&str),
{
    let opt_closure: &&Fn(&str) = unsafe { &*(closure as *const &dyn std::ops::Fn(&str)) };
    let s = unsafe { CStr::from_ptr(v).to_str().unwrap_or_default() };
    opt_closure(s)
}

extern "C" fn cb_start_wrapper<F>(
    closure: *mut c_void,
    engine: *const c_char,
    config: *const c_char,
    msg: *const c_char,
) where
    F: Fn(*const c_char, *const c_char, *const c_char),
{
    let opt_closure: &&StartFn =
        unsafe { &*(closure as *const &(dyn std::ops::Fn(*const i8, *const i8, *const i8))) };
    opt_closure(engine, config, msg);
}

extern "C" fn cb_start_string_wrapper<F>(
    closure: *mut c_void,
    engine: *const c_char,
    config: *const c_char,
    msg: *const c_char,
) where
    F: Fn(&str, &str, &str),
{
    let opt_closure: &&StartStringFn =
        unsafe { &*(closure as *const &(dyn std::ops::Fn(&str, &str, &str) + 'static)) };
    let (engine_str, config_str, msg_str) = unsafe {
        (
            CStr::from_ptr(engine).to_str().unwrap_or_default(),
            CStr::from_ptr(config).to_str().unwrap_or_default(),
            CStr::from_ptr(msg).to_str().unwrap_or_default(),
        )
    };
    opt_closure(engine_str, config_str, msg_str);
}

extern "C" fn cb_each_wrapper<F>(
    closure: *mut c_void,
    kb: c_int,
    k: *const c_char,
    vb: c_int,
    v: *const c_char,
) where
    F: Fn(&[c_char], &[c_char]),
{
    let opt_closure: &&EachFn =
        unsafe { &*(closure as *const &(dyn std::ops::Fn(&[c_char], &[c_char]))) };
    let (ks, vs) = unsafe {
        (
            slice::from_raw_parts(k, kb as usize),
            slice::from_raw_parts(v, vb as usize),
        )
    };
    opt_closure(ks, vs);
}

extern "C" fn cb_each_string_wrapper<F>(
    closure: *mut c_void,
    _kb: c_int,
    k: *const c_char,
    _vb: c_int,
    v: *const c_char,
) where
    F: Fn(&str, &str),
{
    let opt_closure: &&Fn(&str, &str) =
        unsafe { &*(closure as *const &(dyn std::ops::Fn(&str, &str))) };
    let (k_s, v_s) = unsafe {
        (
            CStr::from_ptr(k).to_str().unwrap_or_default(),
            CStr::from_ptr(v).to_str().unwrap_or_default(),
        )
    };
    opt_closure(k_s, v_s)
}

impl KVEngine {
    pub fn start<F>(engine: &str, config: &str, callback: Option<F>) -> Result<KVEngine>
    where
        F: Fn(*const c_char, *const c_char, *const c_char),
        F: 'static,
    {
        let engine_str = CString::new(engine)?;
        let config_str = CString::new(config)?;
        let kvengine = match callback {
            Some(f) => unsafe {
                let mut cb: &StartFn = &f;
                let cb = &mut cb;
                kvengine_start(
                    cb as *mut _ as *mut c_void,
                    engine_str.as_ptr(),
                    config_str.as_ptr(),
                    Some(cb_start_wrapper::<F>),
                )
            },
            None => unsafe {
                kvengine_start(
                    ::std::ptr::null_mut(),
                    engine_str.as_ptr(),
                    config_str.as_ptr(),
                    None,
                )
            },
        };
        if kvengine.is_null() {
            Err(ErrorKind::Fail.into())
        } else {
            Ok(KVEngine(kvengine))
        }
    }

    pub fn start_string<F>(engine: &str, config: &str, callback: Option<F>) -> Result<KVEngine>
    where
        F: Fn(&str, &str, &str),
        F: 'static,
    {
        let engine_str = CString::new(engine)?;
        let config_str = CString::new(config)?;
        let kvengine = match callback {
            Some(f) => unsafe {
                let mut cb: &StartStringFn = &f;
                let cb = &mut cb;
                kvengine_start(
                    cb as *mut _ as *mut c_void,
                    engine_str.as_ptr(),
                    config_str.as_ptr(),
                    Some(cb_start_string_wrapper::<F>),
                )
            },
            None => unsafe {
                kvengine_start(
                    ::std::ptr::null_mut(),
                    engine_str.as_ptr(),
                    config_str.as_ptr(),
                    None,
                )
            },
        };
        if kvengine.is_null() {
            Err(ErrorKind::Fail.into())
        } else {
            Ok(KVEngine(kvengine))
        }
    }

    pub fn put(&mut self, key: &str, value: &str) -> Result<()> {
        let key_str = CString::new(key)?;
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
            Err(ErrorKind::NotFound(key.to_string()).into())
        }
    }

    pub fn remove(&mut self, key: &str) -> Result<()> {
        let key_str = CString::new(key)?;
        let res =
            unsafe { kvengine_remove(self.0, key_str.to_bytes().len() as i32, key_str.as_ptr()) };
        if res == 1 {
            Ok(())
        } else if res == -1 {
            Err(ErrorKind::Fail.into())
        } else {
            Err(ErrorKind::NotFound(key.to_string()).into())
        }
    }

    pub fn get<F>(&self, key: &str, callback: Option<F>) -> Result<()>
    where
        F: Fn(&[c_char]),
    {
        let key_str = CString::new(key)?;
        match callback {
            Some(f) => unsafe {
                let mut cb: &Fn(&[c_char]) = &f;
                let cb = &mut cb;
                kvengine_get(
                    self.0,
                    cb as *mut _ as *mut c_void,
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

    pub fn get_string<F>(&self, key: &str, callback: Option<F>) -> Result<()>
    where
        F: Fn(&str),
    {
        let key_str = CString::new(key)?;
        match callback {
            Some(f) => unsafe {
                let mut cb: &Fn(&str) = &f;
                let cb = &mut cb;
                kvengine_get(
                    self.0,
                    cb as *mut _ as *mut c_void,
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

    pub fn get_copy(&self, key: &str, max_value_bytes: i32) -> Result<String> {
        let key_str = CString::new(key)?;
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
            Err(ErrorKind::NotFound(key.to_string()).into())
        }
    }

    pub fn exists(&self, key: &str) -> Result<()> {
        let key_str = CString::new(key)?;
        let res =
            unsafe { kvengine_exists(self.0, key_str.to_bytes().len() as i32, key_str.as_ptr()) };
        if res == 1 {
            Ok(())
        } else if res == -1 {
            Err(ErrorKind::Fail.into())
        } else {
            Err(ErrorKind::NotFound(key.to_string()).into())
        }
    }

    pub fn each<F>(&self, callback: Option<F>)
    where
        F: Fn(&[c_char], &[c_char]),
        F: 'static,
    {
        match callback {
            Some(f) => unsafe {
                let mut cb: &EachFn = &f;
                let cb = &mut cb;
                kvengine_each(
                    self.0,
                    cb as *mut _ as *mut c_void,
                    Some(cb_each_wrapper::<F>),
                )
            },
            None => unsafe { kvengine_each(self.0, ::std::ptr::null_mut(), None) },
        }
    }

    pub fn each_string<F>(&self, callback: Option<F>)
    where
        F: Fn(&str, &str),
    {
        match callback {
            Some(f) => unsafe {
                let mut cb: &Fn(&str, &str) = &f;
                let cb = &mut cb;
                kvengine_each(
                    self.0,
                    cb as *mut _ as *mut c_void,
                    Some(cb_each_string_wrapper::<F>),
                )
            },
            None => unsafe { kvengine_each(self.0, ::std::ptr::null_mut(), None) },
        }
    }

    pub fn each_above<F>(&self, key: &str, callback: Option<F>) -> Result<()>
    where
        F: Fn(&[c_char], &[c_char]),
        F: 'static,
    {
        let key_str = CString::new(key)?;
        match callback {
            Some(f) => unsafe {
                let mut cb: &EachFn = &f;
                let cb = &mut cb;
                kvengine_each_above(
                    self.0,
                    cb as *mut _ as *mut c_void,
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

    pub fn each_above_string<F>(&self, key: &str, callback: Option<F>) -> Result<()>
    where
        F: Fn(&str, &str),
    {
        let key_str = CString::new(key)?;
        match callback {
            Some(f) => unsafe {
                let mut cb: &Fn(&str, &str) = &f;
                let cb = &mut cb;
                kvengine_each_above(
                    self.0,
                    cb as *mut _ as *mut c_void,
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

    pub fn each_below<F>(&self, key: &str, callback: Option<F>) -> Result<()>
    where
        F: Fn(&[c_char], &[c_char]),
        F: 'static,
    {
        let key_str = CString::new(key)?;
        match callback {
            Some(f) => unsafe {
                let mut cb: &EachFn = &f;
                let cb = &mut cb;
                kvengine_each_below(
                    self.0,
                    cb as *mut _ as *mut c_void,
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

    pub fn each_below_string<F>(&self, key: &str, callback: Option<F>) -> Result<()>
    where
        F: Fn(&str, &str),
    {
        let key_str = CString::new(key)?;
        match callback {
            Some(f) => unsafe {
                let mut cb: &Fn(&str, &str) = &f;
                let cb = &mut cb;
                kvengine_each_below(
                    self.0,
                    cb as *mut _ as *mut c_void,
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

    pub fn each_between<F>(&self, key1: &str, key2: &str, callback: Option<F>) -> Result<()>
    where
        F: Fn(&[c_char], &[c_char]),
        F: 'static,
    {
        let key1_str = CString::new(key1)?;
        let key2_str = CString::new(key2)?;
        match callback {
            Some(f) => unsafe {
                let mut cb: &EachFn = &f;
                let cb = &mut cb;
                kvengine_each_between(
                    self.0,
                    cb as *mut _ as *mut c_void,
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

    pub fn each_between_string<F>(&self, key1: &str, key2: &str, callback: Option<F>) -> Result<()>
    where
        F: Fn(&str, &str),
    {
        let key1_str = CString::new(key1)?;
        let key2_str = CString::new(key2)?;
        match callback {
            Some(f) => unsafe {
                let mut cb: &Fn(&str, &str) = &f;
                let cb = &mut cb;
                kvengine_each_between(
                    self.0,
                    cb as *mut _ as *mut c_void,
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

    pub fn count_above(&self, key: &str) -> Result<i64> {
        let key_str = CString::new(key)?;
        Ok(unsafe {
            kvengine_count_above(self.0, key_str.to_bytes().len() as i32, key_str.as_ptr())
        })
    }

    pub fn count_below(&self, key: &str) -> Result<i64> {
        let key_str = CString::new(key)?;
        Ok(unsafe {
            kvengine_count_below(self.0, key_str.to_bytes().len() as i32, key_str.as_ptr())
        })
    }

    pub fn count_between(&self, key1: &str, key2: &str) -> Result<i64> {
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
        F: Fn(&[c_char]),
    {
        match callback {
            Some(f) => unsafe {
                let mut cb: &F = &f;
                let cb = &mut cb;
                kvengine_all(self.0, cb as *mut _ as *mut c_void, Some(cb_wrapper::<F>))
            },
            None => unsafe { kvengine_all(self.0, ::std::ptr::null_mut(), None) },
        }
    }

    pub fn all_string<F>(&mut self, callback: Option<F>)
    where
        F: Fn(&str),
    {
        match callback {
            Some(f) => unsafe {
                let mut cb: &Fn(&str) = &f;
                let cb = &mut cb;
                kvengine_all(
                    self.0,
                    cb as *mut _ as *mut c_void,
                    Some(cb_string_wrapper::<F>),
                )
            },
            None => unsafe { kvengine_all(self.0, ::std::ptr::null_mut(), None) },
        }
    }

    pub fn all_above<F>(&mut self, key: &str, callback: Option<F>) -> Result<()>
    where
        F: Fn(&[c_char]),
    {
        let key_str = CString::new(key)?;
        match callback {
            Some(f) => {
                unsafe {
                    let mut cb: &Fn(&[c_char]) = &f;
                    let cb = &mut cb;
                    kvengine_all_above(
                        self.0,
                        cb as *mut _ as *mut c_void,
                        key_str.to_bytes().len() as i32,
                        key_str.as_ptr(),
                        Some(cb_wrapper::<F>),
                    )
                }
                Ok(())
            }
            None => {
                unsafe {
                    kvengine_all_above(
                        self.0,
                        ::std::ptr::null_mut(),
                        key_str.to_bytes().len() as i32,
                        key_str.as_ptr(),
                        None,
                    )
                }
                Ok(())
            }
        }
    }

    pub fn all_above_string<F>(&mut self, key: &str, callback: Option<F>) -> Result<()>
    where
        F: Fn(&str),
    {
        let key_str = CString::new(key)?;
        match callback {
            Some(f) => {
                unsafe {
                    let mut cb: &Fn(&str) = &f;
                    let cb = &mut cb;
                    kvengine_all_above(
                        self.0,
                        cb as *mut _ as *mut c_void,
                        key_str.to_bytes().len() as i32,
                        key_str.as_ptr(),
                        Some(cb_string_wrapper::<F>),
                    )
                }
                Ok(())
            }
            None => {
                unsafe {
                    kvengine_all_above(
                        self.0,
                        ::std::ptr::null_mut(),
                        key_str.to_bytes().len() as i32,
                        key_str.as_ptr(),
                        None,
                    )
                }
                Ok(())
            }
        }
    }

    pub fn all_below<F>(&mut self, key: &str, callback: Option<F>) -> Result<()>
    where
        F: Fn(&[c_char]),
    {
        let key_str = CString::new(key)?;
        match callback {
            Some(f) => {
                unsafe {
                    let mut cb: &Fn(&[c_char]) = &f;
                    let cb = &mut cb;
                    kvengine_all_below(
                        self.0,
                        cb as *mut _ as *mut c_void,
                        key_str.to_bytes().len() as i32,
                        key_str.as_ptr(),
                        Some(cb_wrapper::<F>),
                    )
                }
                Ok(())
            }
            None => {
                unsafe {
                    kvengine_all_below(
                        self.0,
                        ::std::ptr::null_mut(),
                        key_str.to_bytes().len() as i32,
                        key_str.as_ptr(),
                        None,
                    )
                }
                Ok(())
            }
        }
    }

    pub fn all_below_string<F>(&mut self, key: &str, callback: Option<F>) -> Result<()>
    where
        F: Fn(&str),
    {
        let key_str = CString::new(key)?;
        match callback {
            Some(f) => {
                unsafe {
                    let mut cb: &Fn(&str) = &f;
                    let cb = &mut cb;
                    kvengine_all_below(
                        self.0,
                        cb as *mut _ as *mut c_void,
                        key_str.to_bytes().len() as i32,
                        key_str.as_ptr(),
                        Some(cb_string_wrapper::<F>),
                    )
                }
                Ok(())
            }
            None => {
                unsafe {
                    kvengine_all_below(
                        self.0,
                        ::std::ptr::null_mut(),
                        key_str.to_bytes().len() as i32,
                        key_str.as_ptr(),
                        None,
                    )
                }
                Ok(())
            }
        }
    }

    pub fn all_between<F>(&mut self, key1: &str, key2: &str, callback: Option<F>) -> Result<()>
    where
        F: Fn(&[c_char]),
    {
        let key1_str = CString::new(key1)?;
        let key2_str = CString::new(key2)?;
        match callback {
            Some(f) => {
                unsafe {
                    let mut cb: &Fn(&[c_char]) = &f;
                    let cb = &mut cb;
                    kvengine_all_between(
                        self.0,
                        cb as *mut _ as *mut c_void,
                        key1_str.to_bytes().len() as i32,
                        key1_str.as_ptr(),
                        key2_str.to_bytes().len() as i32,
                        key2_str.as_ptr(),
                        Some(cb_wrapper::<F>),
                    )
                }
                Ok(())
            }
            None => {
                unsafe {
                    kvengine_all_between(
                        self.0,
                        ::std::ptr::null_mut(),
                        key1_str.to_bytes().len() as i32,
                        key1_str.as_ptr(),
                        key2_str.to_bytes().len() as i32,
                        key2_str.as_ptr(),
                        None,
                    )
                }
                Ok(())
            }
        }
    }

    pub fn all_between_string<F>(
        &mut self,
        key1: &str,
        key2: &str,
        callback: Option<F>,
    ) -> Result<()>
    where
        F: Fn(&str),
    {
        let key1_str = CString::new(key1)?;
        let key2_str = CString::new(key2)?;
        match callback {
            Some(f) => {
                unsafe {
                    let mut cb: &Fn(&str) = &f;
                    let cb = &mut cb;
                    kvengine_all_between(
                        self.0,
                        cb as *mut _ as *mut c_void,
                        key1_str.to_bytes().len() as i32,
                        key1_str.as_ptr(),
                        key2_str.to_bytes().len() as i32,
                        key2_str.as_ptr(),
                        Some(cb_string_wrapper::<F>),
                    )
                }
                Ok(())
            }
            None => {
                unsafe {
                    kvengine_all_between(
                        self.0,
                        ::std::ptr::null_mut(),
                        key1_str.to_bytes().len() as i32,
                        key1_str.as_ptr(),
                        key2_str.to_bytes().len() as i32,
                        key2_str.as_ptr(),
                        None,
                    )
                }
                Ok(())
            }
        }
    }
}
