cc_library(
    name = "index",
    hdrs = ["index.h"],
    visibility = ["//visibility:public"],
    deps = [
        "//backend:structure",
        "//backend/index",
        "//common:hash",
        "//common:memory_usage",
        "//common:type",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_test(
    name = "index_test",
    srcs = ["index_test.cc"],
    deps = [
        ":index",
        "//backend/index",
        "//backend/index:test_util",
        "//common:status_test_util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "linear_hash_index",
    hdrs = ["linear_hash_index.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":linear_hash_map",
        "//backend:structure",
        "//common:hash",
        "//common:memory_usage",
        "//common:type",
        "@com_google_absl//absl/hash",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_test(
    name = "linear_hash_index_test",
    srcs = ["linear_hash_index_test.cc"],
    deps = [
        ":linear_hash_index",
        "//backend/index",
        "//backend/index:test_util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "linear_hash_map",
    hdrs = ["linear_hash_map.h"],
    deps = [
        "//common:memory_usage",
        "@com_google_absl//absl/hash",
    ],
)

cc_test(
    name = "linear_hash_map_test",
    srcs = ["linear_hash_map_test.cc"],
    deps = [
        ":linear_hash_map",
        "@com_google_googletest//:gtest_main",
    ],
)
