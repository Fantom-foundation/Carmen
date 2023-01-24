cc_library(
    name = "multimap",
    hdrs = ["multimap.h"],
    visibility = ["//visibility:public"],
    deps = [
        "//backend/common:file",
        "//common:fstream",
        "//common:type",
        "@com_google_absl//absl/container:btree",
        "@com_google_absl//absl/strings",
    ],
)

cc_test(
    name = "multimap_test",
    srcs = ["multimap_test.cc"],
    deps = [
        ":multimap",
        "//backend/multimap",
        "//common:file_util",
        "//common:status_test_util",
        "//common:type",
        "@com_google_googletest//:gtest_main",
    ],
)
