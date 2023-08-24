# Carmen Go
This directory contains a prototype Go implementation of the Carmen storage system.

# Build
Some parts parts of the system depend on C++ libraries in the repositories cpp directory. To build those, you need to install `clang` and `bazel` as outlined in ./cpp/README.md and run
```
go generate ./...
```
in this work directory. Note that the `go` build tool is not designed to pick up on changes in the generator's dependencies. Thus, updates of the C++ library need to be performed manually every time the C++ implementation evolvs.

## Installing gomock
If running the generator command above reports that `gomock` is not installed on your system you can install it using the following command:
```
go install go.uber.org/mock/mockgen@latest
```

# Formatting
Before commiting any changes in the Go source files into the repository, please ensure they are correctly formatted by running:
```
gofmt -s -w .
```

# Running benchmarks
Benchmarks use Go "testing" package. To run for example the Stores benchmark, run:
```
go test ./backend/store -bench=/.*File.*_16
```
For more information about the regex selecting benchmarks to run,
check [Go testing documentation](https://pkg.go.dev/testing#hdr-Subtests_and_Sub_benchmarks).

