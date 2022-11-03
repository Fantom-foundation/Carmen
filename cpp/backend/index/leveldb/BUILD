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
