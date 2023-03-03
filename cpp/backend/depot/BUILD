cc_library(
    name = "depot",
    hdrs = ["depot.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":snapshot",
        "//backend:structure",
        "//common:type",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_library(
    name = "snapshot",
    srcs = ["snapshot.cc"],
    hdrs = ["snapshot.h"],
    visibility = ["//backend/depot:__subpackages__"],
    deps = [
        "//backend:snapshot",
        "//common:hash",
        "//common:status_util",
        "//common:type",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_test(
    name = "snapshot_test",
    srcs = ["snapshot_test.cc"],
    deps = [
        ":snapshot",
        "//backend:snapshot",
        "//common:status_test_util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "depot_handler",
    testonly = True,
    hdrs = ["depot_handler.h"],
    deps = [
        ":depot",
        "//backend/depot/cache",
        "//backend/depot/memory:depot",
        "//common:file_util",
        "//common:status_util",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_library(
    name = "test_util",
    testonly = True,
    hdrs = ["test_util.h"],
    visibility = ["//visibility:public"],
    deps = [
        "//common:memory_usage",
        "//common:type",
        "@com_google_googletest//:gtest",
    ],
)

cc_library(
    name = "depot_test_suite",
    testonly = True,
    hdrs = ["depot_test_suite.h"],
    visibility = ["//backend/depot:__subpackages__"],
    deps = [
        ":depot",
        ":depot_handler",
        "//common:status_test_util",
        "//common:test_util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_binary(
    name = "depot_benchmark",
    testonly = True,
    srcs = ["depot_benchmark.cc"],
    deps = [
        ":depot_handler",
        "//backend/depot/cache",
        "//backend/depot/file:depot",
        "//backend/depot/leveldb:depot",
        "//backend/depot/memory:depot",
        "//common:benchmark",
        "//common:status_test_util",
        "//third_party/gperftools:profiler",
        "@com_github_google_benchmark//:benchmark_main",
    ],
)
