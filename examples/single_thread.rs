extern crate mlkv_rust;

use std::ffi::CString;
use mlkv_rust::{FasterKv, faster_status};

fn main() {
    let filename = CString::new("/tmp/testdb").unwrap();
    let store = FasterKv::new(16 * 1024 * 1024, 16 * 1024 * 1024 * 1024, filename);
    let mut cur_key: u64;
    let mut cur_value: u64 = 0;
    let incr: u64 = 5;

    let value_ptr = Box::into_raw(Box::new(cur_value)) as *mut u8;
    let incr_ptr = Box::into_raw(Box::new(incr)) as *mut u8;

    // Upsert
    for i in 0..1000 {
        cur_key = i;
        cur_value = i + 1000;
        unsafe { *(value_ptr as *mut u64) = cur_value; }
        let upsert = store.upsert(cur_key, value_ptr, std::mem::size_of_val(&cur_value) as u64);
        assert!(upsert == faster_status::FasterStatus::OK as u8);
    }

    // Read-Modify-Write
    for i in 0..1000 {
        cur_key = i;
        let rmw = store.rmw(cur_key, incr_ptr, std::mem::size_of_val(&cur_value) as u64);
        assert!(rmw == faster_status::FasterStatus::OK as u8);
    }

    // Read
    for i in 0..1000 {
        cur_key = i;
        let read = store.read(cur_key, value_ptr);
        // let read = store.mlkv_read(cur_key, value_ptr, std::mem::size_of_val(&cur_value) as u64);
        // let upsert = store.mlkv_upsert(cur_key, value_ptr, std::mem::size_of_val(&cur_value) as u64);
        // let read = store.mlkv_lookahead(cur_key, std::mem::size_of_val(&cur_value) as u64);
        assert!(read == faster_status::FasterStatus::OK as u8);
        unsafe { println!("Key: {}, Value: {}", cur_key, *(value_ptr as *mut u64)); }
    }

    // Delete
    for i in 0..1000 {
        cur_key = i;
        let delete = store.delete(cur_key);
        assert!(delete == faster_status::FasterStatus::OK as u8);
    }

    // Read
    for i in 0..1000 {
        cur_key = i;
        let read = store.read(cur_key, value_ptr);
        assert!(read == faster_status::FasterStatus::NotFound as u8);
    }

    // Clear used storage
    match store.clean_storage() {
        Ok(()) => {}
        Err(_err) => panic!("Unable to clear FASTER directory"),
    }
}
