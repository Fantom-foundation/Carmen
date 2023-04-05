#!/bin/bash
# This script builds the C++ state library to be linked into a Go application
# and copies it into the ./go/lib directory.
#
# To refresh the libraries, you may either run the script directly or run 
# `go generate ./state`.
#
# Note that you have to set up your Docker or C++ build environment.
# Setup C++ build environment (not the IDE) according to ./cpp/README,
# the `bazel` command must be in one of our $PATH directories.
#
set -e

cd "$(dirname $0)/../../cpp"

# Use clang compiler unless overruled.
export CC=${CC:-clang}
export CXX=${CXX:-clang++}

if bazel version &> /dev/null
then
    echo "- C++ build environment:"
    bazel build -c opt //state:libcarmen.so
    mkdir -p ../go/lib
    rm -f ../go/lib/libcarmen.so
    cp ./bazel-bin/state/libcarmen.so ../go/lib/
else
    echo "- Docker build environment:"
    cd ..
    docker run \
        --rm \
        -v $(pwd):/src \
        -w /src/go/lib \
        --entrypoint=/bin/bash \
        golang:1.19 \
        -c "go install github.com/bazelbuild/bazelisk@v1.15.0 && ln -s /go/bin/bazelisk /go/bin/bazel && apt update && apt install -y clang && ./build_libcarmen.sh"
fi