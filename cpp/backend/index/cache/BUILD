cc_library(
    name = "cache",
    hdrs = ["cache.h"],
    visibility = ["//visibility:public"],
    deps = [
        "//backend:structure",
        "//backend/common/cache:lru_cache",
        "//backend/index",
        "//common:hash",
        "//common:memory_usage",
        "//common:status_util",
        "//common:type",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_test(
    name = "cache_test",
    srcs = ["cache_test.cc"],
    deps = [
        ":cache",
        "//backend/index:test_util",
        "//backend/index/memory:index",
        "//common:status_test_util",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
    ],
)
