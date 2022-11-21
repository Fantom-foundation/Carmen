cc_library(
    name = "depot",
    hdrs = ["depot.h"],
    deps = [
        "//common:type",
        "//common:hash"
    ],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "depot_handler",
    hdrs = ["depot_handler.h"],
    deps = [
        ":depot",
        "//backend/depot/memory:depot",
        "//common:file_util"
    ],
    testonly = True,
)

cc_test(
    name = "depot_test",
    srcs = ["depot_test.cc"],
    deps = [
        ":depot",
        ":depot_handler",
        "//backend/depot/memory:depot",
        "//backend/depot/leveldb:depot",
        "//backend/depot/file:depot",
        "//common:status_test_util",
        "//common:test_util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_binary(
    name = "depot_benchmark",
    srcs = ["depot_benchmark.cc"],
    deps = [
        ":depot_handler",
        "//backend/depot/memory:depot",
        "//backend/depot/file:depot",
        "//backend/depot/leveldb:depot",
        "//common:benchmark",
        "@com_github_google_benchmark//:benchmark_main",
        "//third_party/gperftools:profiler",
    ],
    testonly = True,
)
