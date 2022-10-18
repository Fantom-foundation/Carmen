# Project Carmen
This directory contains a C++ implementation of the Carmen storage system.

# Needed Tools
To build the C++ implementation you need the following tools:
 - a C++ compiler supporting C++20 (recommended clang 14+)
 - the bazel build tool

## Installing bazel
We recommend the usage of `Bazelisk` which can be installed as a Go tool using
```
go install github.com/bazelbuild/bazelisk@latest
```
Once installed, the `bazelisk` binary can be found in your go tool folder. If
`$GOPATH` is set, the binary should be located at `$GOPATH/bin/bazelisk`. If
not, it will default to `~/go/bin/bazelisk`.

The `bazelisk` binary is a drop-in replacement for the `bazel` command which
automatically fetches a `bazel` version according to the target project's
requirements for you. However, to make it accessible as a `bazel` command, a
symbolic link must be created.

To do so, pick a directory that is listed in your `$PATH` environment variable
and add a symbolic named `bazel` in this directory using
```
ln -s <path_to_bazelisk> bazel
```
For instance, if `~/go/bin` is in your `$PATH` environment variable any
`bazelisk` has been installed there, run
```
ln -s ~/go/bin/bazelisk ~/go/bin/bazel
```
to get access to `bazel` in your command line.

# Build and Test
To build the full project, use the following commands:
```
bazel build //...
```

For this to work you will have to have `bazel` installed on your system,
as well as a C++ compiler toolchain supporting the C++20 standard. We
recommend using clang.

To run all unit tests, run
```
bazel test //...
```

Individual targets can be build using commands similar to
```
bazel run //common:type_test
```

# Profiling
To profile and visualize profiled data, we recommend using the `pprof`.
To install it as Go tool, run:
```
go install github.com/google/pprof@latest
```

The binary will be installed in `$GOPATH/bin` (`$HOME/go/bin` by default). To 
make it accessible as a `pprof` command, a symbolic link or alias must be created.
```
alias pprof=<path_to_pprof>
```
To make alias persistent, put it into your `.bashrc` or `.zshrc` file.

Link profiler (`//third_party/gperftools:profiler`) into the target binary you 
want to profile.

Example:
```
cc_binary(
    name = "eviction_policy_benchmark",
    srcs = ["eviction_policy_benchmark.cc"],
    deps = [
        ":eviction_policy",
        "@com_github_google_benchmark//:benchmark_main",
        "//third_party/gperftools:profiler",
    ],
)
```
To start collection of profiling data, run the binary with the `CPUPROFILE` 
environment variable set to the path of the output file. For example:
```
CPUPROFILE=/tmp/profile.dat bazel run -c opt //backend/store:store_benchmark -- --benchmark_filter=HashExponential.*File.*/16
```

To visualize the collected data (`graphviz` has to be installed), run:
```
pprof --http=":8000" /tmp/profile.dat
```

# Setting up your IDE
The setup of your development environment depends on the IDE of your choice.

## Visual Studio Code
To set up your VS code, install the following extensions:
 - [Bazel](https://marketplace.visualstudio.com/items?itemName=BazelBuild.vscode-bazel)
 - [bazel-stack-vscode](https://marketplace.visualstudio.com/items?itemName=StackBuild.bazel-stack-vscode)
 - [bazel-stack-vscode-cc](https://marketplace.visualstudio.com/items?itemName=StackBuild.bazel-stack-vscode-cc)
 - [C/C++](https://marketplace.visualstudio.com/items?itemName=ms-vscode.cpptools)
 - [clangd](https://marketplace.visualstudio.com/items?itemName=llvm-vs-code-extensions.vscode-clangd)
 - [Clang-Format](https://marketplace.visualstudio.com/items?itemName=xaver.clang-format)

To open the project, use the *cpp* directory as the project root in VS code.

Once everything is up and running, open the command panel (Ctrl+Shift+P) and run the command
```
Bazel/C++: Generate Compilation Database
```
This will generate a `compile_commands.json` file in the cpp directory listing local code
dependencies pulled in by the bazel build system. IntelliSense is using this file to locate
source source files of dependencies. This file is specific for your environment and should
not be checked in into the repository.

With this, VS code should be set up to support editing code with proper code completion and
navigation.

If you encounter issues with this description, feel free to updated and send a pull request.