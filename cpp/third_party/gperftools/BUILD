# Copyright (c) 2024 Fantom Foundation
# 
# Use of this software is governed by the Business Source License included
# in the LICENSE file and at fantom.foundation/bsl11.
# 
# Change Date: 2028-4-16
# 
# On the date above, in accordance with the Business Source License, use of
# this software will be governed by the GNU Lesser General Public License v3.

load("@rules_foreign_cc//foreign_cc:defs.bzl", "cmake")

cmake(
    name = "profiler",
    build_args = [
        "--verbose",
        "--",  # <- Pass remaining options to the native tool.
        "-j 8",
    ],
    copts = ["-Wno-error"],
    generate_args = [
        "-Dgperftools_build_benchmark=OFF",
    ],
    lib_source = "@gperftools//:all_srcs",
    out_shared_libs = select({
        # TODO: test on windows if needed
        "@platforms//os:windows": [
            "libprofiler.lib",
        ],
        "@platforms//os:osx": [
            "libprofiler.5.5.5.dylib",
        ],
        "//conditions:default": [
            "libprofiler.so.5.5.5",
            #"libfake_stacktrace_scope.a",
            #"liblogging.a",
            #"libmaybe_threads.a",
            #"libprofiler.a",
            #"libspinlock.a",
            #"libsysinfo.a",
        ],
    }),
    visibility = ["//visibility:public"],
    alwayslink = True,
)
