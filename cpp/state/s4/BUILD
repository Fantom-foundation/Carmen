# This module contains a Carmen Schema implementation modeling an
# MPT like structure supporting destructive updates.

cc_library(
    name = "state",
    hdrs = ["state.h"],
    visibility = [
        "//state:__subpackages__",
    ],
    deps = [
        ":nodes",
        "//state:schema",
        "//state:update",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_test(
    name = "state_test",
    srcs = ["state_test.cc"],
    deps = [
        ":state",
        "//state:configurations",
        "//state:state_test_suite",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "nodes",
    hdrs = ["nodes.h"],
    deps = [
        "//common:hash",
        "//common:memory_usage",
        "//common:status_util",
        "//common:type",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
    ],
)

cc_test(
    name = "nodes_test",
    srcs = ["nodes_test.cc"],
    deps = [
        ":nodes",
        "//common:status_test_util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_binary(
    name = "nodes_benchmark",
    testonly = True,
    srcs = ["nodes_benchmark.cc"],
    deps = [
        ":nodes",
        "//common:status_test_util",
        "//third_party/gperftools:profiler",
        "@com_github_google_benchmark//:benchmark_main",
    ],
)
