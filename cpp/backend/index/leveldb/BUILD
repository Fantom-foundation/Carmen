cc_library(
    name = "index",
    hdrs = ["index.h"],
    deps = [
        "//common:type",
        "//common:hash",
        "//backend/common/leveldb:level_db",
        "//common:status_util",
        "@com_google_absl//absl/status:statusor"
    ],
    visibility = ["//backend/index/leveldb:__subpackages__"],
)

cc_binary(
    name = "index_benchmark",
    srcs = ["index_benchmark.cc"],
    deps = [
        "//backend/index/leveldb/single_db:index",
        "//backend/index/leveldb/multi_db:index",
        "//common:file_util",
        "//common:type",
        "@com_github_google_benchmark//:benchmark_main",
        "//third_party/gperftools:profiler",
    ],
    testonly = True,
)
