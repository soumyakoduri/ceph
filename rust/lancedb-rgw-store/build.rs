/*
 * Ceph - scalable distributed file system
 *
 * Copyright 2026 IBM
 *
 * See file COPYING for licensing information.
 */

//! Build script for lancedb-rgw-store
//!
//! This script handles linking to Ceph libraries when building
//! with real SAL support (i.e., without the mock-sal feature).

fn main() {
    // When not using mock-sal, the Rust functions (rgw_put_object, etc.) are
    // resolved at runtime. The symbols are provided by rgw_sal_wrapper.cc which
    // is compiled into libradosgw. Since this crate produces a shared library
    // (.so) that's loaded into the same process as radosgw, the symbols will
    // be available at runtime.
    //
    // We do NOT statically link here to avoid circular dependency:
    // - lancedb-rgw-store depends on rgw_sal_wrapper symbols
    // - rgw_common depends on liblancedb_rgw_store.so
    //
    // By using runtime symbol resolution (undefined symbols in .so are allowed),
    // both can be built independently and linked together in radosgw.
    #[cfg(not(feature = "mock-sal"))]
    {
        // Re-run if environment changes
        println!("cargo:rerun-if-env-changed=CEPH_BUILD_DIR");
        println!("cargo:rerun-if-env-changed=CEPH_SRC_DIR");
    }

    // Always re-run if the build script itself changes
    println!("cargo:rerun-if-changed=build.rs");
}
