# Introduction

This directory contains the Go implementation of Carmen.
It includes the officially supported Merkle Patricia Trie State Root Hash compatible
implementation. 
Furthermore, a few experimental implementations are available.   

# Build

Carmen is included in other systems as a Go library.
The Go implementation does not need any C++ dependency, 
i.e.; no build is required to use the library. 

# Integrate

Carmen is added to another project simply as a dependency:

```
go get github.com/Fantom-foundation/Carmen/go/
```

# Development

For development purposes, it may come handy to execute all tests. It needs build of c++ parts.

Either install c++ build environment, see [[C++ build environment instructions|../cpp/Readme]],
or have [Docker installation](https://www.docker.com)

Execute in the root directory: 
```
make 
```

Then execute in this directory: 
```
go test ./...  -timeout=60m  
```

On memory constrained systems, parallelism may need to be reduced
```
go test ./... -parallel=1  -timeout 600m   
```
all tests should pass. 

## Installing gomock

Tests extensively use mocks. 
To regenerate mocks, use the command:

```
go generate ./...
```

If this command reports that `gomock` is not installed, one can install it using the following command:

```
go install go.uber.org/mock/mockgen@latest
```

## Formatting

Formatting rules are enforced in the devvelopment process by running:
```
gofmt -s -w .
```

## Benchmarks

Some of the key parts may be exercised with benchmarks. 
For example, the Stores benchmark is executed as:
```
go test ./backend/store -bench=/.*File.*_16
```
For more information about the running benchmarks see: 
[Go testing documentation](https://pkg.go.dev/testing#hdr-Benchmarks).

