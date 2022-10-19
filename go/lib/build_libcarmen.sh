#!/bin/bash
# This script builds the C++ state library to be linked into a Go application
# and copies it into the ./go/lib directory.
#
# To refresh the libraries, you may either run the script directly or run 
# `go generate ./state`.
#
# Note that in either case you have to set up your C++ build environment (not
# the IDE) according to ./cpp/README. In particular, the `bazel` command must
# be in on of our $PATH directories.
#
set -e
cd ../../cpp
bazel build -c opt //state:libcarmen.so
mkdir -p ../go/lib
rm -f ../go/lib/libcarmen.so
cp ./bazel-bin/state/libcarmen.so ../go/lib