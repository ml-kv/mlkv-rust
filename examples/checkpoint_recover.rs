extern crate rust_boringml;

use std::{fs, path::Path};
use std::ffi::CString;
use rust_boringml::{FasterKv, faster_status};

fn main() {
    let filename = CString::new("./testdb").unwrap();
    let mut store = std::sync::Arc::new(FasterKv::new(16 * 1024 * 1024, 4 * 1024 * 1024 * 1024, filename.clone()));
    let num_threads = 8;

    // Parallel upsert
    let handles: Vec<std::thread::JoinHandle<_>> = (0..num_threads)
        .map(|thread_id| {
            let store = store.clone();
            std::thread::spawn(move || {
                let value_ptr = Box::into_raw(Box::new(0 as u64)) as *mut u8;

                store.start_session();
                for i in 0..50000000 {
                    let cur_key: u64 = thread_id * 50000000 + i;
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

    // Checkpoint
    match store.checkpoint() {
        true => println!("Checkpoint successfully!"),
        false => panic!("Unable to checkpoint FASTER directory!"),
    }

    drop(store);

    let mut checkpoint_token: Option<CString> = None;
    for entry in fs::read_dir(Path::new("./testdb/cpr-checkpoints")).unwrap() {
        let relative_path = entry.unwrap().file_name();
        checkpoint_token = CString::new(relative_path.to_str().unwrap()).ok();
        break;
    }

    match checkpoint_token {
        None => { panic!("Checkpoint not found"); },
        Some(_token) => {
            store = std::sync::Arc::new(FasterKv::recover(16 * 1024 * 1024, 8 * 1024 * 1024 * 1024, filename, _token.clone()));
            println!("Recover from checkpoint token : {:?}", _token);
            // Read
            let mut cur_key: u64;
            let value_ptr = Box::into_raw(Box::new(0 as u64)) as *mut u8;

            store.start_session();
            for i in 0..100 {
                cur_key = i;
                let read = store.read(cur_key, value_ptr);
                if read == faster_status::FasterStatus::Pending as u8 {
                    store.complete_pending(true);
                } else {
                    assert!(read == faster_status::FasterStatus::OK as u8);
                }
                unsafe { println!("Key: {}, Value: {}", cur_key, *(value_ptr as *mut u64)); }
            }
            store.stop_session();
        }
    }

    // Clear used storage
    match store.clean_storage() {
        Ok(()) => {}
        Err(_err) => panic!("Unable to clear FASTER directory"),
    }
}
