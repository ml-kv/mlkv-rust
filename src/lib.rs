extern crate libfaster_sys as ffi;

mod faster_error;
pub mod faster_status;

pub use crate::faster_error::FasterError;

use std::ffi::CString;
use std::fs;

pub struct FasterKv {
    faster_t: *mut ffi::faster_t,
    filename: Option<String>,
}

impl FasterKv {
    pub fn upsert(&self, key: u64, value_ptr: *mut u8, value_length: u64) -> u8 {
        unsafe {
            ffi::faster_upsert(
                self.faster_t,
                key,
                value_ptr,
                value_length,
            )
        }
    }

    pub fn read(&self, key: u64, value_ptr: *mut u8) -> u8 {
        unsafe {
            ffi::faster_read(
                self.faster_t,
                key,
                value_ptr
            )
        }
    }

    pub fn rmw(&self, key: u64, incr_ptr: *mut u8, value_length: u64) -> u8 {
        unsafe {
            ffi::faster_rmw(
                self.faster_t,
                key,
                incr_ptr,
                value_length,
            )
        }
    }

    pub fn delete(&self, key: u64) -> u8
    {
        unsafe {
            ffi::faster_delete(
                self.faster_t,
                key,
            )
        }
    }

    pub fn mlkv_read(&self, key: u64, value_ptr: *mut u8, value_length: u64) -> u8 {
        unsafe {
            ffi::mlkv_read(
                self.faster_t,
                key,
                value_ptr,
                value_length,
            )
        }
    }

    pub fn mlkv_upsert(&self, key: u64, value_ptr: *mut u8, value_length: u64) -> u8 {
        unsafe {
            ffi::mlkv_upsert(
                self.faster_t,
                key,
                value_ptr,
                value_length,
            )
        }
    }

    pub fn mlkv_lookahead(&self, key: u64, value_length: u64) -> u8 {
        unsafe {
            ffi::mlkv_lookahead(
                self.faster_t,
                key,
                value_length,
            )
        }
    }

    pub fn start_session(&self) -> () {
        unsafe { ffi::faster_start_session(self.faster_t) }
    }

    pub fn stop_session(&self) -> () {
        unsafe { ffi::faster_stop_session(self.faster_t) }
    }

    pub fn complete_pending(&self, wait : bool) -> () {
        unsafe { ffi::faster_complete_pending(self.faster_t, wait) }
    }

    pub fn recover(table_size_bytes : u64, log_size_bytes : u64, filename : CString, checkpoint_token : CString) -> Self {
        unsafe {
            let store = ffi::faster_recover(table_size_bytes,
                                            log_size_bytes,
                                            filename.clone().into_raw(),
                                            checkpoint_token.clone().into_raw());
            FasterKv {faster_t : store, filename : filename.into_string().ok()}
        }
    }

    pub fn checkpoint(&self) -> bool {
        unsafe { ffi::faster_checkpoint( self.faster_t ) }
    }

    // Warning: Calling this will remove the stored data
    pub fn clean_storage(&self) -> Result<(), FasterError> {
        match &self.filename {
            None => Err(FasterError::InvalidType),
            Some(dir) => {
                fs::remove_dir_all(dir)?;
                Ok(())
            }
        }
    }

    pub fn new(table_size_bytes : u64, log_size_bytes : u64, filename : CString) -> Self {
        unsafe {
            let store = ffi::faster_open(table_size_bytes, log_size_bytes, filename.clone().into_raw());
            FasterKv {faster_t : store, filename : filename.into_string().ok()}
        }
    }

    fn destroy(&self) -> () {
        unsafe {
            ffi::faster_destroy(self.faster_t);
        }
    }
}

// In order to make sure we release the resources the C interface has allocated for the store
impl Drop for FasterKv {
    fn drop(&mut self) {
        self.destroy();
    }
}

unsafe impl Send for FasterKv {}
unsafe impl Sync for FasterKv {}
