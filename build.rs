fn main() {
    // if we're on Mac OS X we'll kindly add DYLD_FALLBACK_LIBRARY_PATH to rustc's
    // linker search path
    if let Some(dyld_fallback_path) = option_env!("DYLD_FALLBACK_LIBRARY_PATH") {
        println!("cargo:rustc-link-search=native={}", dyld_fallback_path)
    }
}
