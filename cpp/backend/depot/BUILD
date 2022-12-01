cc_library(
    name = "depot",
    hdrs = ["depot.h"],
    deps = [
        "//backend:structure",
        "//common:type",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
    ],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "depot_handler",
    hdrs = ["depot_handler.h"],
    deps = [
        ":depot",
        "//backend/depot/memory:depot",
        "//backend/depot/cache:cache",
        "//common:file_util"
    ],
    testonly = True,
)

cc_library(
    name = "test_util",
    hdrs = ["test_util.h"],
    deps = [
        "//common:type",
        "//common:memory_usage",
        "@com_google_googletest//:gtest",
    ],
    visibility = ["//backend/depot:__subpackages__"],
    testonly = True,
)

cc_test(
    name = "depot_test",
    srcs = ["depot_test.cc"],
    deps = [
        ":depot",
        ":depot_handler",
        "//backend/depot/cache:cache",
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
        "//backend/depot/cache:cache",
        "//common:benchmark",
        "@com_github_google_benchmark//:benchmark_main",
        "//third_party/gperftools:profiler",
    ],
    testonly = True,
)
