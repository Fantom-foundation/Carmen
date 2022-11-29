cc_library(
    name = "lru_cache",
    hdrs = ["lru_cache.h"],
    deps = [
        "//common:memory_usage",
        "@com_google_absl//absl/container:flat_hash_map",
    ],
    visibility = ["//backend:__subpackages__"],
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
    srcs = ["lru_cache_benchmark.cc"],
    deps = [
        ":lru_cache",
        "//common:benchmark",
        "@com_github_google_benchmark//:benchmark_main",
        "//third_party/gperftools:profiler",
    ],
    testonly = True,
)
