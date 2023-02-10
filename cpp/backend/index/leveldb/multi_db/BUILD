cc_library(
    name = "index",
    hdrs = ["index.h"],
    visibility = ["//visibility:public"],
    deps = [
        "//backend/common/leveldb",
        "//backend/index/leveldb:index",
    ],
)

cc_test(
    name = "index_test",
    srcs = ["index_test.cc"],
    deps = [
        ":index",
        "//backend/index",
        "//backend/index:index_test_suite",
        "//common:file_util",
        "//common:status_test_util",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
    ],
)
