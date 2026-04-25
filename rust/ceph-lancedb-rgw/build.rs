// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Ceph Authors

//! Build script for ceph-lancedb-rgw
//!
//! This script handles linking to Ceph libraries when building
//! with real SAL support (i.e., without the mock-sal feature).

fn main() {
    // When not using mock-sal, we need to link to Ceph libraries
    #[cfg(not(feature = "mock-sal"))]
    {
        // Try to find Ceph build directory from environment
        if let Ok(ceph_build_dir) = std::env::var("CEPH_BUILD_DIR") {
            println!("cargo:rustc-link-search=native={}/lib", ceph_build_dir);
            println!("cargo:rustc-link-search=native={}/lib/rgw", ceph_build_dir);
        }

        // Try to find Ceph source directory for header includes
        if let Ok(ceph_src_dir) = std::env::var("CEPH_SRC_DIR") {
            println!("cargo:include={}/src", ceph_src_dir);
        }

        // Link to the SAL LanceDB wrapper library
        // This is built from ceph/src/rgw/rgw_sal_lancedb_wrapper.cc
        println!("cargo:rustc-link-lib=static=rgw_sal_lancedb_wrapper");

        // Link to required RGW libraries
        println!("cargo:rustc-link-lib=static=rgw_common");
        println!("cargo:rustc-link-lib=static=rgw_sal");

        // Link to Ceph common libraries
        println!("cargo:rustc-link-lib=static=ceph-common");
        println!("cargo:rustc-link-lib=static=common");

        // Note: RADOS is not directly linked - SAL abstracts over backends
        // and handles RADOS internally through rgw_sal if the backend uses it

        // Link to standard C++ library
        println!("cargo:rustc-link-lib=dylib=stdc++");

        // Re-run if environment changes
        println!("cargo:rerun-if-env-changed=CEPH_BUILD_DIR");
        println!("cargo:rerun-if-env-changed=CEPH_SRC_DIR");
    }

    // Always re-run if the build script itself changes
    println!("cargo:rerun-if-changed=build.rs");
}
