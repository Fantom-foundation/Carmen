# Carmen Go
This directory contains a prototype Go implementation of the Carmen storage system.

# Build
Some parts parts of the system depend on C++ libraries in the repositories cpp directory. To build those, you need to install `clang` and `bazel` as outlined in ./cpp/README.md and run
```
go generate ./...
```
in this work directory. Note that the `go` build tool is not designed to pick up on changes in the generator's dependencies. Thus, updates of the C++ library need to be performed manually every time the C++ implementation evolvs.

# Formatting
Before commiting any changes in the Go source files into the repository, please ensure they are correctly formatted by running:
```
gofmt -s -w .
```
