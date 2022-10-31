#!/bin/bash
#
# Run this script in any directory to auto-format all files in the current 
# work directory and its sub-directories.
#
find ./ -iname *.h -o -iname *.cc | xargs --no-run-if-empty clang-format -i
gofmt -s -w .

