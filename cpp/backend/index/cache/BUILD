cc_library(
    name = "cache",
    hdrs = ["cache.h"],
    deps = [
        "//backend/common/cache:lru_cache",
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
