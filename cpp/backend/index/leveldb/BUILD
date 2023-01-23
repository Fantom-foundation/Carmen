cc_library(
    name = "index",
    hdrs = ["index.h"],
    visibility = ["//backend/index/leveldb:__subpackages__"],
    deps = [
        "//backend:structure",
        "//backend/common/leveldb",
        "//common:hash",
        "//common:memory_usage",
        "//common:status_util",
        "//common:type",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_binary(
    name = "index_benchmark",
    testonly = True,
    srcs = ["index_benchmark.cc"],
    deps = [
        "//backend/index/leveldb/multi_db:index",
        "//backend/index/leveldb/single_db:index",
        "//common:file_util",
        "//common:type",
        "//common:status_test_util",
        "//third_party/gperftools:profiler",
        "@com_github_google_benchmark//:benchmark_main",
    ],
)
