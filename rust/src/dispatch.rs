use std::ffi::{CString, c_char, c_int};
use std::os::unix::ffi::OsStringExt;
use std::os::unix::fs::MetadataExt;

unsafe extern "C" {
    fn execv(path: *const c_char, argv: *const *const c_char) -> c_int;
}

/// Replace this process with the sibling build. If the exec fails, the caller
/// continues in the original binary with the original arguments and environment.
pub(super) fn exec_v4() {
    let Ok(path) = std::env::current_exe() else {
        return;
    };
    let sibling = path.with_file_name("rproto-v4");
    let (Ok(current), Ok(next)) = (
        std::fs::metadata("/proc/self/exe"),
        std::fs::metadata(&sibling),
    ) else {
        return;
    };
    // A mistakenly linked copy of the launcher would otherwise restart itself.
    if !next.is_file() || (current.dev(), current.ino()) == (next.dev(), next.ino()) {
        return;
    }
    let Ok(path) = CString::new(sibling.into_os_string().into_vec()) else {
        return;
    };
    let Ok(args) = std::env::args_os()
        .map(|arg| CString::new(arg.into_vec()))
        .collect::<Result<Vec<_>, _>>()
    else {
        return;
    };
    let mut argv: Vec<_> = args.iter().map(|arg| arg.as_ptr()).collect();
    argv.push(std::ptr::null());
    // Both C strings and the null-terminated pointer array live through execv.
    // The absolute path and execv avoid a PATH search or shell fallback.
    unsafe { execv(path.as_ptr(), argv.as_ptr()) };
}
