===============
s3vectors Tests
===============

* Start the cluster using the `vstart.sh` script
* Run the test from within the `src/test/rgw/s3vectors` directory:
  `S3VTESTS_CONF=s3vtests.conf.SAMPLE tox`
* To run a specific tests use:
  `S3VTESTS_CONF=s3vtests.conf.SAMPLE tox -- s3vector_test.py::<test_name>`
* To run a group of tests use:
  `S3VTESTS_CONF=s3vtests.conf.SAMPLE tox -- s3vector_test.py -m "<marker name>"`
* In case of multisite environment, you can set a "secondary" site in the conf file. See: `s3vtests.conf.multisite`

Configuration Options
=====================

The following options can be set in the config file under the `[DEFAULT]` section:

+-------------------+----------+---------+---------------------------------------------------+
| Option            | Required | Default | Description                                       |
+===================+==========+=========+===================================================+
| host              | Yes      | -       | RGW endpoint hostname                             |
+-------------------+----------+---------+---------------------------------------------------+
| port              | Yes      | -       | RGW endpoint port                                 |
+-------------------+----------+---------+---------------------------------------------------+
| zonegroup         | Yes      | -       | RGW zonegroup name                                |
+-------------------+----------+---------+---------------------------------------------------+
| s3vector_backend  | No       | local   | Storage backend for s3vector data.                |
|                   |          |         | Options: ``local``, ``s3``, or ``sal``            |
+-------------------+----------+---------+---------------------------------------------------+

S3/SAL Backend Mode
-------------------

When ``s3vector_backend`` is set to ``s3`` or ``sal``, the tests will automatically
create a regular S3 bucket with the same name as the vector bucket before performing
vector operations. This is required because these backends store LanceDB data
directly in an S3 bucket with the vector bucket name.

Example configuration for S3 backend::

    [DEFAULT]
    port = 8000
    host = localhost
    zonegroup = default
    s3vector_backend = s3

    [s3 main]
    access_key = 0555b35654ad1656d804
    secret_key = h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q==

