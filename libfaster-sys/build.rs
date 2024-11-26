extern crate bindgen;

use std::env;
use std::path::PathBuf;
use cmake::Config;

fn faster_bindgen() {
    let bindings = bindgen::Builder::default()
        .header("faster_c.h")
        .blacklist_type("max_align_t") // https://github.com/rust-lang-nursery/rust-bindgen/issues/550
        .ctypes_prefix("libc")
        .generate()
        .expect("unable to generate faster bindings");

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("unable to write faster bindings");
}


fn main() {
    faster_bindgen();

    let dst = Config::new("FASTER/cc")
        .cflag("--std=c++14")
        .build();

    println!("cargo:rustc-link-search=native={}/{}", dst.display(), "build");
    // Fix this...
    println!("cargo:rustc-link-lib=static=faster");
    println!("cargo:rustc-link-lib=stdc++fs");
    println!("cargo:rustc-link-lib=uuid");
    println!("cargo:rustc-link-lib=tbb");
    println!("cargo:rustc-link-lib=gcc");
    println!("cargo:rustc-link-lib=aio");
    println!("cargo:rustc-link-lib=m");
    println!("cargo:rustc-link-lib=stdc++");
    println!("cargo:rustc-link-lib=pthread");
}
