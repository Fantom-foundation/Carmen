cc_library(
    name = "cache",
    hdrs = ["cache.h"],
    deps = [
        ":lru_cache",
        "//backend/index",
        "//common:type",
        "//common:hash",
        "//common:memory_usage",
        "@com_google_absl//absl/container:flat_hash_map",
    ],
    visibility = ["//visibility:public"],
)

cc_test(
    name = "cache_test",
    srcs = ["cache_test.cc"],
    deps = [
        ":cache", 
        "//backend/index:test_util",
        "//backend/index/memory:index",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "lru_cache",
    hdrs = ["lru_cache.h"],
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
    srcs = ["lru_cache_benchmark.cc"],
    deps = [
        ":lru_cache",
        "//common:benchmark",
        "@com_github_google_benchmark//:benchmark_main",
        "//third_party/gperftools:profiler",
    ],
    testonly = True,
)
