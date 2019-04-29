extern crate pmemkv;

fn start_failure_callback(_engine: &str, _config: &str, msg: &str) {
    eprint!("ERROR: {}\n", msg);
    ::std::process::exit(1);
}

fn main() {
    let mut kv = pmemkv::kvengine::KVEngine::start_string(
        "vsmap",
        "{\"path\":\"/mnt/mem/\"}",
        Some(start_failure_callback),
    )
    .unwrap();
    let res = kv.put("key1", "value1");
    assert!(res.is_ok() && kv.count() == 1);
    kv.put("key2", "value2").unwrap();
    kv.put("key3", "value3").unwrap();
    assert!(kv.count() == 3);
    let s = kv.get_copy("key2", 10).unwrap();
    assert!(s == "value2");
    kv.all_string(Some(|s: &str| println!("{}", s)));
    let res = kv.remove("key1");
    assert!(res.is_ok());
    let res = kv.exists("key1");
    assert!(res.is_err());
    let err = res.unwrap_err();
    use pmemkv::errors::ErrorKind;
    match err.kind() {
        ErrorKind::NotFound(k) => assert_eq!(k, "key1"),
        ErrorKind::Fail => panic!("fail to check the existence"),
        _ => panic!("should throw not found error"),
    }
}
