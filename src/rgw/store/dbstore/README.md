# dbstore
DBstore for Rados Gateway (RGW)

## Pre-install

fmt(-devel) and gtest(-devel) packages need to be installed

## Build

cd src/rgw/store/dbstore

mkdir build

cd build

cmake ../

[To enable debug symbols, pass "-DDEBUG_SYMS=ON" option to cmake]

make


## Execute

./dbstore
