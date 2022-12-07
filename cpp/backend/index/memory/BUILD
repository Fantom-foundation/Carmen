cc_library(
    name = "index",
    hdrs = ["index.h"],
    deps = [
        "//backend/index",
        "//backend:structure",
        "//common:type",
        "//common:hash",
        "//common:memory_usage",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/status:status",
    ],
    visibility = ["//visibility:public"],
)

cc_test(
    name = "index_test",
    srcs = ["index_test.cc"],
    deps = [
        ":index", 
        "//backend/index",
        "//common:status_test_util",
        "//backend/index:test_util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "linear_hash_index",
    hdrs = ["linear_hash_index.h"],
    deps = [
        ":linear_hash_map",
        "//backend:structure",
        "//common:type",
        "//common:hash",
        "//common:memory_usage",
        "@com_google_absl//absl/hash",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/status:status",
    ],
    visibility = ["//visibility:public"],
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
