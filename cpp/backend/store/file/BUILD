cc_library(
    name = "eviction_policy",
    hdrs = ["eviction_policy.h"],
    srcs = ["eviction_policy.cc"],
    deps = [
        "@com_google_absl//absl/container:node_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
    ]
)

cc_test(
    name = "eviction_policy_test",
    srcs = ["eviction_policy_test.cc"],
    deps = [
        ":eviction_policy",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_binary(
    name = "eviction_policy_benchmark",
    srcs = ["eviction_policy_benchmark.cc"],
    deps = [
        ":eviction_policy",
        "@com_github_google_benchmark//:benchmark_main",
        "//third_party/gperftools:profiler",
    ],
)

cc_library(
    name = "hash_tree",
    hdrs = ["hash_tree.h"],
    srcs = ["hash_tree.cc"],
    deps = [
        "//backend/common:page",
        "//backend/common:page_id",
        "//common:type",
        "//common:hash",
        "@com_google_absl//absl/container:flat_hash_set",
    ]
)

cc_test(
    name = "hash_tree_test",
    srcs = ["hash_tree_test.cc"],
    deps = [
        ":hash_tree",
        "//common:type",
        "//common:hash",
        "//common:file_util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "store",
    hdrs = ["store.h"],
    deps = [
        ":hash_tree",
        "//backend/common:file",
        "//backend/common:page_pool",
        "//common:type",
        "//common:hash",
    ],
    visibility = ["//visibility:public"],
)

cc_test(
    name = "store_test",
    srcs = ["store_test.cc"],
    deps = [
        ":store",
        "@com_google_googletest//:gtest_main",
    ],
)
