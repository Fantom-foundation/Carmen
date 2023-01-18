cc_library(
    name = "index",
    hdrs = ["index.h"],
    visibility = ["//visibility:public"],
    deps = [
        "//backend:structure",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_test(
    name = "index_test",
    srcs = ["index_test.cc"],
    deps = [
        "//backend/index/memory:index",
        "//common:status_test_util",
        "//common:test_util",
        "//common:type",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "test_util",
    testonly = True,
    hdrs = ["test_util.h"],
    visibility = ["//backend/index:__subpackages__"],
    deps = [
        ":index",
        "//backend:structure",
        "//backend/index:index_handler",
        "//backend/index/memory:index",
        "//common:hash",
        "//common:status_test_util",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest",
    ],
)

cc_test(
    name = "test_util_test",
    srcs = ["test_util_test.cc"],
    deps = [
        ":index",
        ":test_util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "index_handler",
    testonly = True,
    hdrs = ["index_handler.h"],
    visibility = ["//backend/index:__subpackages__"],
    deps = [
        ":index",
        "//backend/index/cache",
        "//backend/index/file:index",
        "//backend/index/leveldb/multi_db:index",
        "//backend/index/leveldb/single_db:index",
        "//common:file_util",
        "//common:type",
    ],
)

cc_binary(
    name = "index_benchmark",
    testonly = True,
    srcs = ["index_benchmark.cc"],
    deps = [
        ":index_handler",
        "//backend/common:file",
        "//backend/index/cache",
        "//backend/index/file:index",
        "//backend/index/memory:index",
        "//backend/index/memory:linear_hash_index",
        "//common:benchmark",
        "//common:status_test_util",
        "//third_party/gperftools:profiler",
        "@com_github_google_benchmark//:benchmark_main",
    ],
)
