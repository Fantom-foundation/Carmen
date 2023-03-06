cc_library(
    name = "structure",
    hdrs = ["structure.h"],
    visibility = ["//visibility:public"],
    deps = [
        "//common:heterogenous_map",
        "//common:memory_usage",
        "//common:type",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_library(
    name = "snapshot",
    hdrs = ["snapshot.h"],
    visibility = ["//visibility:public"],
    deps = [
        "//common:type",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_library(
    name = "snapshot_test_utils",
    testonly = True,
    hdrs = ["snapshot_test_utils.h"],
    deps = [
        "//common:hash",
        "//common:status_util",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_test(
    name = "snapshot_test_utils_test",
    srcs = ["snapshot_test_utils_test.cc"],
    visibility = ["//visibility:public"],
    deps = [
        ":snapshot",
        ":snapshot_test_utils",
        "//common:hash",
        "//common:status_test_util",
        "//common:type",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "union_snapshot",
    hdrs = ["union_snapshot.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":snapshot",
        "//common:status_util",
        "//common:type",
        "//common:variant_util",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_test(
    name = "union_snapshot_test",
    srcs = ["union_snapshot_test.cc"],
    visibility = ["//visibility:public"],
    deps = [
        ":snapshot",
        ":snapshot_test_utils",
        ":union_snapshot",
        "//common:hash",
        "//common:status_test_util",
        "//common:type",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
    ],
)
