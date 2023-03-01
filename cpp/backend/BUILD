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

cc_test(
    name = "snapshot_test",
    srcs = ["snapshot_test.cc"],
    visibility = ["//visibility:public"],
    deps = [
        ":snapshot",
        "//common:hash",
        "//common:status_test_util",
        "//common:type",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
    ],
)
