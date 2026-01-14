/**
 * @file rgw_sal_c_example.c
 * @brief Example program demonstrating the RGW SAL C API
 *
 * This example shows how to use the C bindings for the RGW Storage
 * Abstraction Layer (SAL) to perform basic operations like:
 *   - Creating and initializing a driver
 *   - Creating and managing users
 *   - Creating and listing buckets
 *   - Creating and managing objects
 *
 * To compile (after building Ceph):
 *   gcc -o rgw_sal_c_example rgw_sal_c_example.c \
 *       -I/path/to/ceph/src/include \
 *       -L/path/to/ceph/build/lib \
 *       -lrgw_sal -lrados -lpthread
 *
 * To run:
 *   export LD_LIBRARY_PATH=/path/to/ceph/build/lib:$LD_LIBRARY_PATH
 *   ./rgw_sal_c_example [ceph.conf path]
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <rgw/rgw_sal_c.h>

/* Helper macro for error checking */
#define CHECK_RET(ret, msg) \
    do { \
        if ((ret) < 0) { \
            fprintf(stderr, "Error: %s (ret=%d)\n", (msg), (ret)); \
            goto cleanup; \
        } \
    } while(0)

/* Print version info */
static void print_version(void)
{
    int major, minor, extra;
    const char* version = rgw_sal_version(&major, &minor, &extra);
    printf("RGW SAL C Library: %s\n", version);
    printf("Version: %d.%d.%d\n\n", major, minor, extra);
}

/* Example: Create a driver and get cluster info */
static int example_driver_info(const char* conf_path)
{
    rgw_sal_driver_t driver = NULL;
    struct rgw_sal_cluster_stat stats;
    char cluster_id[256];
    const char* driver_name;
    int ret;

    printf("=== Driver Info Example ===\n");

    /* Create the driver */
    ret = rgw_sal_driver_create("rados", conf_path, &driver);
    CHECK_RET(ret, "Failed to create driver");

    /* Initialize the driver */
    ret = rgw_sal_driver_initialize(driver);
    CHECK_RET(ret, "Failed to initialize driver");

    /* Get driver name */
    driver_name = rgw_sal_driver_get_name(driver);
    printf("Driver name: %s\n", driver_name ? driver_name : "unknown");

    /* Get cluster ID */
    ret = rgw_sal_driver_get_cluster_id(driver, cluster_id, sizeof(cluster_id));
    if (ret == 0) {
        printf("Cluster ID: %s\n", cluster_id);
    }

    /* Get cluster statistics */
    ret = rgw_sal_driver_get_cluster_stat(driver, &stats);
    if (ret == 0) {
        printf("Cluster Stats:\n");
        printf("  Total: %lu KB\n", (unsigned long)stats.kb);
        printf("  Used:  %lu KB\n", (unsigned long)stats.kb_used);
        printf("  Free:  %lu KB\n", (unsigned long)stats.kb_avail);
        printf("  Objects: %lu\n", (unsigned long)stats.num_objects);
    }

    ret = 0;
    printf("Driver info example completed successfully.\n\n");

cleanup:
    if (driver) {
        rgw_sal_driver_destroy(driver);
    }
    return ret;
}

/* Example: User operations */
static int example_user_operations(const char* conf_path)
{
    rgw_sal_driver_t driver = NULL;
    rgw_sal_user_t user = NULL;
    struct rgw_sal_user_info user_info;
    char user_id[256];
    char display_name[256];
    int ret;

    printf("=== User Operations Example ===\n");

    memset(&user_info, 0, sizeof(user_info));

    /* Create and initialize driver */
    ret = rgw_sal_driver_create("rados", conf_path, &driver);
    CHECK_RET(ret, "Failed to create driver");

    ret = rgw_sal_driver_initialize(driver);
    CHECK_RET(ret, "Failed to initialize driver");

    /* Get a user by ID */
    ret = rgw_sal_get_user(driver, "testuser", NULL, &user);
    if (ret < 0) {
        printf("User 'testuser' not found (ret=%d) - this is expected for new clusters\n", ret);
        /* Continue with example */
        ret = 0;
        goto cleanup;
    }

    /* Get user ID */
    ret = rgw_sal_user_get_id(user, user_id, sizeof(user_id));
    if (ret == 0) {
        printf("User ID: %s\n", user_id);
    }

    /* Get display name */
    ret = rgw_sal_user_get_display_name(user, display_name, sizeof(display_name));
    if (ret == 0) {
        printf("Display Name: %s\n", display_name);
    }

    /* Load full user info */
    ret = rgw_sal_user_load(user, &user_info);
    if (ret == 0) {
        printf("User Info:\n");
        printf("  User ID: %s\n", user_info.user_id ? user_info.user_id : "");
        printf("  Tenant: %s\n", user_info.tenant ? user_info.tenant : "");
        printf("  Display Name: %s\n", user_info.display_name ? user_info.display_name : "");
        printf("  Email: %s\n", user_info.email ? user_info.email : "");
        printf("  Max Buckets: %u\n", user_info.max_buckets);
        printf("  Suspended: %s\n", user_info.suspended ? "yes" : "no");
        
        /* Free user info strings */
        rgw_sal_user_info_free(&user_info);
    }

    ret = 0;
    printf("User operations example completed.\n\n");

cleanup:
    if (user) {
        rgw_sal_user_destroy(user);
    }
    if (driver) {
        rgw_sal_driver_destroy(driver);
    }
    return ret;
}

/* Example: Bucket operations */
static int example_bucket_operations(const char* conf_path)
{
    rgw_sal_driver_t driver = NULL;
    rgw_sal_bucket_t bucket = NULL;
    struct rgw_sal_bucket_info bucket_info;
    struct rgw_sal_bucket_list bucket_list;
    char bucket_name[256];
    int ret;
    size_t i;

    printf("=== Bucket Operations Example ===\n");

    memset(&bucket_info, 0, sizeof(bucket_info));
    memset(&bucket_list, 0, sizeof(bucket_list));

    /* Create and initialize driver */
    ret = rgw_sal_driver_create("rados", conf_path, &driver);
    CHECK_RET(ret, "Failed to create driver");

    ret = rgw_sal_driver_initialize(driver);
    CHECK_RET(ret, "Failed to initialize driver");

    /* List buckets for a user */
    ret = rgw_sal_list_buckets(driver, "testuser", NULL, NULL, 100, &bucket_list);
    if (ret == 0) {
        printf("Found %zu bucket(s) for user 'testuser':\n", bucket_list.count);
        for (i = 0; i < bucket_list.count; i++) {
            printf("  - %s (tenant: %s, size: %lu)\n",
                   bucket_list.buckets[i].name ? bucket_list.buckets[i].name : "",
                   bucket_list.buckets[i].tenant ? bucket_list.buckets[i].tenant : "",
                   (unsigned long)bucket_list.buckets[i].size);
        }
        if (bucket_list.is_truncated) {
            printf("  ... (more buckets, next_marker: %s)\n", 
                   bucket_list.next_marker ? bucket_list.next_marker : "");
        }
        rgw_sal_bucket_list_free(&bucket_list);
    } else {
        printf("No buckets found or user doesn't exist (ret=%d)\n", ret);
    }

    /* Try to get a specific bucket */
    ret = rgw_sal_get_bucket(driver, "testbucket", NULL, &bucket);
    if (ret == 0) {
        /* Get bucket name */
        ret = rgw_sal_bucket_get_name(bucket, bucket_name, sizeof(bucket_name));
        if (ret == 0) {
            printf("Got bucket: %s\n", bucket_name);
        }

        /* Load bucket info */
        ret = rgw_sal_bucket_load(bucket, &bucket_info);
        if (ret == 0) {
            printf("Bucket Info:\n");
            printf("  Name: %s\n", bucket_info.name ? bucket_info.name : "");
            printf("  Tenant: %s\n", bucket_info.tenant ? bucket_info.tenant : "");
            printf("  Bucket ID: %s\n", bucket_info.bucket_id ? bucket_info.bucket_id : "");
            printf("  Owner: %s\n", bucket_info.owner_id ? bucket_info.owner_id : "");
            printf("  Size: %lu\n", (unsigned long)bucket_info.size);
            
            rgw_sal_bucket_info_free(&bucket_info);
        }

        rgw_sal_bucket_destroy(bucket);
        bucket = NULL;
    } else {
        printf("Bucket 'testbucket' not found (ret=%d) - this is expected\n", ret);
    }

    ret = 0;
    printf("Bucket operations example completed.\n\n");

cleanup:
    if (bucket) {
        rgw_sal_bucket_destroy(bucket);
    }
    if (driver) {
        rgw_sal_driver_destroy(driver);
    }
    return ret;
}

/* Example: Object operations */
static int example_object_operations(const char* conf_path)
{
    rgw_sal_driver_t driver = NULL;
    rgw_sal_bucket_t bucket = NULL;
    rgw_sal_object_t object = NULL;
    struct rgw_sal_object_info object_info;
    char object_name[256];
    int ret;

    printf("=== Object Operations Example ===\n");

    memset(&object_info, 0, sizeof(object_info));

    /* Create and initialize driver */
    ret = rgw_sal_driver_create("rados", conf_path, &driver);
    CHECK_RET(ret, "Failed to create driver");

    ret = rgw_sal_driver_initialize(driver);
    CHECK_RET(ret, "Failed to initialize driver");

    /* Get a bucket */
    ret = rgw_sal_get_bucket(driver, "testbucket", NULL, &bucket);
    if (ret < 0) {
        printf("Bucket 'testbucket' not found - skipping object operations\n");
        ret = 0;
        goto cleanup;
    }

    /* Get an object from the bucket */
    ret = rgw_sal_bucket_get_object(bucket, "testobject.txt", NULL, &object);
    if (ret < 0) {
        printf("Creating new object 'testobject.txt'\n");
        ret = rgw_sal_bucket_create_object(bucket, "testobject.txt", &object);
        if (ret < 0) {
            printf("Failed to create object (ret=%d)\n", ret);
            goto cleanup;
        }
    }

    /* Get object name */
    ret = rgw_sal_object_get_name(object, object_name, sizeof(object_name));
    if (ret == 0) {
        printf("Object name: %s\n", object_name);
    }

    /* Load object info */
    ret = rgw_sal_object_load(object, &object_info);
    if (ret == 0) {
        printf("Object Info:\n");
        printf("  Name: %s\n", object_info.name ? object_info.name : "");
        printf("  Instance: %s\n", object_info.instance ? object_info.instance : "(current)");
        printf("  Size: %lu\n", (unsigned long)object_info.size);
        printf("  ETag: %s\n", object_info.etag ? object_info.etag : "");
        printf("  Content-Type: %s\n", object_info.content_type ? object_info.content_type : "");
        
        rgw_sal_object_info_free(&object_info);
    }

    ret = 0;
    printf("Object operations example completed.\n\n");

cleanup:
    if (object) {
        rgw_sal_object_destroy(object);
    }
    if (bucket) {
        rgw_sal_bucket_destroy(bucket);
    }
    if (driver) {
        rgw_sal_driver_destroy(driver);
    }
    return ret;
}

/* Main function */
int main(int argc, char** argv)
{
    const char* conf_path = NULL;
    int ret;

    printf("RGW SAL C Interface Example\n");
    printf("============================\n\n");

    /* Parse arguments */
    if (argc > 1) {
        conf_path = argv[1];
        printf("Using config file: %s\n\n", conf_path);
    } else {
        printf("No config file specified, using defaults\n");
        printf("Usage: %s [ceph.conf]\n\n", argv[0]);
    }

    /* Print version */
    print_version();

    /* Run examples */
    ret = example_driver_info(conf_path);
    if (ret < 0) {
        fprintf(stderr, "Driver info example failed\n");
    }

    ret = example_user_operations(conf_path);
    if (ret < 0) {
        fprintf(stderr, "User operations example failed\n");
    }

    ret = example_bucket_operations(conf_path);
    if (ret < 0) {
        fprintf(stderr, "Bucket operations example failed\n");
    }

    ret = example_object_operations(conf_path);
    if (ret < 0) {
        fprintf(stderr, "Object operations example failed\n");
    }

    printf("============================\n");
    printf("Examples completed.\n");

    return 0;
}
