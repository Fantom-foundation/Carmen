cc_library(
    name = "store",
    hdrs = ["store.h"],
    deps = [
        "//backend/common:page_id",
    ],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "store_handler",
    hdrs = ["store_handler.h"],
    deps = [
        "//backend/store/memory:store",
        "//backend/store/file:store",
        "//common:file_util",
        "//common:type", 
    ],
    testonly = True,
)

cc_test(
    name = "store_test",
    srcs = ["store_test.cc"],
    deps = [
        ":store_handler",
        "//backend/store/memory:store",
        "//backend/store/file:store",
        "//common:test_util",
        "//common:type", 
        "@com_google_googletest//:gtest_main",
    ],
)

cc_binary(
    name = "store_benchmark",
    srcs = ["store_benchmark.cc"],
    deps = [
        ":store_handler",
        "//backend/store/memory:store",
        "//backend/store/file:store",
        "@com_github_google_benchmark//:benchmark_main",
        "//third_party/gperftools:profiler",
    ],
    testonly = True,
)

cc_library(
    name = "hash_tree",
    hdrs = ["hash_tree.h"],
    srcs = ["hash_tree.cc"],
    deps = [
        "//backend/common:page",
        "//backend/common:page_id",
        "//backend/common/leveldb:level_db",
        "//common:type",
        "//common:hash",
        "//common:status_util",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/status:status",
    ],
    visibility = ["//backend:__subpackages__"],
)

cc_test(
    name = "hash_tree_test",
    srcs = ["hash_tree_test.cc"],
    deps = [
        ":hash_tree",
        "//common:type",
        "//common:hash",
        "//common:file_util",
        "//common:status_test_util",
        "@com_google_googletest//:gtest_main",
    ],
)
