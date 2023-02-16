cc_library(
    name = "archive",
    hdrs = ["archive.h"],
    visibility = ["//visibility:public"],
    deps = [
        "//backend:structure",
        "//common:type",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_library(
    name = "test_util",
    testonly = True,
    hdrs = ["test_util.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":archive",
        "//common:memory_usage",
        "//common:type",
        "//state:update",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest",
    ],
)
