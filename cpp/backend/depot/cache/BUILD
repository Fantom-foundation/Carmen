cc_library(
    name = "cache",
    hdrs = ["cache.h"],
    deps = [
        "//backend/common/cache:lru_cache",
        "//backend/depot",
        "//common:type",
        "//common:memory_usage",
        "//common:status_util",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status:status",
        "@com_google_absl//absl/status:statusor",
    ],
    visibility = ["//visibility:public"],
)

cc_test(
    name = "cache_test",
    srcs = ["cache_test.cc"],
    deps = [
        ":cache",
        "//backend/depot:test_util",
        "//common:status_test_util",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/status:status",
        "@com_google_absl//absl/status:statusor",
    ],
)
