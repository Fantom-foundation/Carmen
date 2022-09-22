# Project Carmen
This directory contains a C++ implementation of the Carmen storage system.

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