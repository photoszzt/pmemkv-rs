extern crate pmemkv;

use std::ffi::CStr;
use std::os::raw::{c_char, c_void};

extern "C" fn start_failure_callback(
    _context: *mut c_void,
    _engine: *const c_char,
    _config: *const c_char,
    msg: *const c_char,
) {
    let msg_str = unsafe { CStr::from_ptr(msg).to_string_lossy().into_owned() };
    eprint!("ERROR: {}\n", msg_str);
    ::std::process::exit(1);
}

fn main() {
    let mut kv = pmemkv::kvengine::KVEngine::start(
        ::std::ptr::null_mut(),
        "vsmap".to_string(),
        "{\"path\":\"/mnt/mem/\"}".to_string(),
        Some(start_failure_callback),
    )
    .unwrap();
    let res = kv.put("key1".to_string(), "value1".to_string());
    assert!(res.is_ok() && kv.count() == 1);
    kv.put("key2".to_string(), "value2".to_string()).unwrap();
    kv.put("key3".to_string(), "value3".to_string()).unwrap();
    assert!(kv.count() == 3);
    let s = kv.get_copy("key2".to_string(), 10).unwrap();
    assert!(s == "value2");
    kv.all_string(Some(|s| println!("{}", s)));
    let res = kv.remove("key1".to_string());
    assert!(res.is_ok());
    let res = kv.exists("key1".to_string());
    assert!(res.is_err());
    let err = res.unwrap_err();
    use pmemkv::errors::ErrorKind;
    match err.kind() {
        ErrorKind::NotFound(k) => assert_eq!(k, "key1"),
        ErrorKind::Fail => assert!(false, "fail to check the existence"),
        _ => assert!(false, "should throw not found error"),
    }
}
