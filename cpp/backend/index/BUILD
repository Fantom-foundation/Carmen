cc_library(
    name = "index",
    hdrs = ["index.h"],
    deps = [
        "//common:type",
        "//common:memory_usage",
    ],
    visibility = ["//visibility:public"],
)

cc_test(
    name = "index_test",
    srcs = ["index_test.cc"],
    deps = [
        "//backend/index/memory:index",
        "//common:test_util",
        "//common:type", 
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "test_util",
    hdrs = ["test_util.h"],
    deps = [
        ":index",
        "//common:hash",
        "//backend/index:index_handler",
        "@com_google_googletest//:gtest",
    ],
    visibility = ["//backend/index:__subpackages__"],
    testonly = True,
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
    hdrs = ["index_handler.h"],
    deps = [
        ":index",
        "//common:type",
        "//backend/index/leveldb/single_db:test_util",
        "//backend/index/leveldb/single_db:index",
        "//backend/index/leveldb/multi_db:test_util",
        "//backend/index/leveldb/multi_db:index",
        "//backend/index/cache:cache",
        "//backend/index/file:index",
        "//common:file_util",
    ],
    visibility = ["//backend/index:__subpackages__"],
    testonly = True,
)

cc_binary(
    name = "index_benchmark",
    srcs = ["index_benchmark.cc"],
    deps = [
        ":index_handler",
        "//backend/common:file",
        "//backend/index/cache",
        "//backend/index/file:index",
        "//backend/index/memory:index",
        "//backend/index/memory:linear_hash_index",
        "//common:benchmark",
        "@com_github_google_benchmark//:benchmark_main",
        "//third_party/gperftools:profiler",
    ],
    testonly = True,
)
