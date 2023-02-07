cc_library(
    name = "store",
    hdrs = ["store.h"],
    visibility = ["//visibility:public"],
    deps = [
        "//backend/store",
        "//backend/store:hash_tree",
        "//common:hash",
        "//common:memory_usage",
        "//common:type",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_test(
    name = "store_test",
    srcs = ["store_test.cc"],
    deps = [
        ":store",
        "//common:file_util",
        "//common:status_test_util",
        "@com_google_googletest//:gtest_main",
    ],
)
