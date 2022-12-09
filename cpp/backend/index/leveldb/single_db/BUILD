cc_library(
    name = "index",
    hdrs = ["index.h"],
    srcs = ["index.cc"],
    deps = [
        "//common:type",
        "//common:hash",
        "//backend/common/leveldb:leveldb",
        "//backend/index/leveldb:index",
        "//common:status_util",
        "@com_google_absl//absl/status:statusor"
    ],
    visibility = ["//visibility:public"],
)

cc_test(
    name = "index_test",
    srcs = ["index_test.cc"],
    deps = [
        ":index",
        "//common:file_util",
        "//backend/index",
        "//backend/index:test_util",
        "//common:status_test_util",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/status:statusor"
    ],
)

cc_binary(
    name = "index_benchmark",
    srcs = ["index_benchmark.cc"],
    deps = [
        "//backend/index/leveldb/single_db:index",
        "//common:type",
        "@com_github_google_benchmark//:benchmark_main",
        "//third_party/gperftools:profiler",
    ],
    testonly = True,
)
