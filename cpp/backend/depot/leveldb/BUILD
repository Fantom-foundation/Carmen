cc_library(
    name = "depot",
    hdrs = ["depot.h"],
    visibility = ["//visibility:public"],
    deps = [
        "//backend/common/leveldb",
        "//backend/depot:snapshot",
        "//backend/store:hash_tree",
        "//common:byte_util",
        "//common:hash",
        "//common:status_util",
        "//common:type",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_test(
    name = "depot_test",
    srcs = ["depot_test.cc"],
    deps = [
        ":depot",
        "//backend/depot",
        "//backend/depot:depot_test_suite",
        "//common:file_util",
        "//common:status_test_util",
        "//common:type",
        "@com_google_googletest//:gtest_main",
    ],
)
