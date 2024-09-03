extern crate mlkv_rust;

use std::ffi::CString;
use mlkv_rust::{FasterKv, faster_status};

fn main() {
    let num_threads = 10;
    let filename = CString::new("/tmp/testdb").unwrap();
    let store = std::sync::Arc::new(FasterKv::new(16 * 1024 * 1024, 16 * 1024 * 1024 * 1024, filename));
    // Parallel upsert
    let handles: Vec<std::thread::JoinHandle<_>> = (0..num_threads)
        .map(|thread_id| {
            let store = store.clone();
            std::thread::spawn(move || {
                let value_ptr = Box::into_raw(Box::new(0 as u64)) as *mut u8;

                store.start_session();
                for i in 0..100 {
                    let cur_key: u64 = thread_id * 100 + i;
                    let cur_value: u64 = cur_key + 1000;
                    unsafe { *(value_ptr as *mut u64) = cur_value; }
                    let upsert = store.upsert(cur_key, value_ptr, std::mem::size_of_val(&cur_value) as u64);
                    assert!(upsert == faster_status::FasterStatus::OK as u8);
                }
                store.stop_session();
            })
        }).collect();

    for handle in handles {
        handle.join().unwrap();
    }

    // Parallel read-Modify-Write
    let handles: Vec<std::thread::JoinHandle<_>> = (0..num_threads)
        .map(|thread_id| {
            let store = store.clone();
            std::thread::spawn(move || {
                let incr: u64 = 5;
                let incr_ptr = Box::into_raw(Box::new(incr)) as *mut u8;
                
                store.start_session();
                for i in 0..100 {
                    let cur_key: u64 = thread_id * 100 + i;
                    let rmw = store.rmw(cur_key, incr_ptr, std::mem::size_of_val(&incr) as u64);
                    assert!(rmw == faster_status::FasterStatus::OK as u8);
                }
                store.stop_session();
            })
        }).collect();

    for handle in handles {
        handle.join().unwrap();
    }

    let mut cur_key: u64;
    let cur_value: u64 = 0;
    let value_ptr = Box::into_raw(Box::new(cur_value)) as *mut u8;

    // Read
    for i in 0..1000 {
        cur_key = i;
        let read = store.read(cur_key, value_ptr);
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
    store.stop_session();

    // Clear used storage
    match store.clean_storage() {
        Ok(()) => {}
        Err(_err) => panic!("Unable to clear FASTER directory"),
    }
}
