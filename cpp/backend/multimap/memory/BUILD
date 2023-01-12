
cc_library(
    name = "multimap",
    hdrs = ["multimap.h"],
    deps = [
        "//backend/common:file",
        "//common:type",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/container:btree",
    ],
    visibility = ["//visibility:public"],
)

cc_test(
    name = "multimap_test",
    srcs = ["multimap_test.cc"],
    deps = [
        ":multimap",
        "//backend/multimap",
        "//common:type",
        "//common:file_util",
        "//common:status_test_util",
        "@com_google_googletest//:gtest_main",
    ],
)
