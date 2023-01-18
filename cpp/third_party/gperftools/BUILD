load("@rules_foreign_cc//foreign_cc:defs.bzl", "cmake")

cmake(
    name = "profiler",
    build_args = [
        "--verbose",
        "--",  # <- Pass remaining options to the native tool.
        "-j 8",
    ],
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
