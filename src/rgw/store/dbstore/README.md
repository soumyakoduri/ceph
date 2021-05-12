# dbstore
DBStore for Rados Gateway (RGW)

## Pre-install
fmt(-devel) and gtest(-devel) packages need to be installed

## Build
cd build

ninja src/rgw/store/dbstore/tests/install

[To enable debug symbols, pass "-DDEBUG_SYMS=ON" option to cmake]

## Execute

./bin/dbstore-bin

## Logging
Different loglevels are supported 

ERROR - 0

EVENT - 1

DEBUG - 2

FULLDEBUG - 3

By default log level is set to EVENT and logs are stored in dbstore.log

[To provide custom log file and change log level]

./dbstore-bin log_file log_level

eg., ./dbstore-bin

## Gtests
To execute Gtest cases, from build directory

cd tests

./bin/dbstore-tests

