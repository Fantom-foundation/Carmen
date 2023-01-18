cc_library(
    name = "lru_cache",
    hdrs = ["lru_cache.h"],
    visibility = ["//backend:__subpackages__"],
    deps = [
        "//common:memory_usage",
        "@com_google_absl//absl/container:flat_hash_map",
    ],
)

cc_test(
    name = "lru_cache_test",
    srcs = ["lru_cache_test.cc"],
    deps = [
        ":lru_cache",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_binary(
    name = "lru_cache_benchmark",
    testonly = True,
    srcs = ["lru_cache_benchmark.cc"],
    deps = [
        ":lru_cache",
        "//common:benchmark",
        "//third_party/gperftools:profiler",
        "@com_github_google_benchmark//:benchmark_main",
    ],
)
