# dbstore
DBstore for Rados Gateway (RGW)

## Build

git clone git@github.com:soumyakoduri/dbstore.git

cd dbstore

git submodule update --init

mkdir build

cd build

cmake ../

[To enable debug symbols, pass "-DDEBUG_SYMS=ON" option to cmake]

make


## Execute

./dbstore
