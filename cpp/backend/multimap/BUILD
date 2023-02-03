cc_library(
    name = "multimap",
    hdrs = ["multimap.h"],
    visibility = ["//visibility:public"],
    deps = [
        "//backend:structure",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_library(
    name = "test_util",
    testonly = True,
    hdrs = ["test_util.h"],
    visibility = ["//visibility:public"],
    deps = [
        "//common:type",
        "//common:memory_usage",
        "//backend:structure",
        "//common:status_util",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest",
    ],
)
