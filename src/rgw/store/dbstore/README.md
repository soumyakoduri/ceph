# dbstore
DBstore for Rados Gateway (RGW)

## Build

cd src/rgw/store/dbstore

mkdir build

cd build

cmake ../

[To enable debug symbols, pass "-DDEBUG_SYMS=ON" option to cmake]

make


## Execute

./dbstore
