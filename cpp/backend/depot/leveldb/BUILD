cc_library(
    name = "depot",
    hdrs = ["depot.h"],
    deps = [
        "//backend/store:hash_tree",
        "//common:hash",
        "//common:type",
        "//common:byte_util",
        "//common:status_util",
        "//backend/common/leveldb:leveldb",
        "@com_google_absl//absl/status:status",
        "@com_google_absl//absl/status:statusor",
    ],
    visibility = ["//visibility:public"],
)

cc_test(
    name = "depot_test",
    srcs = ["depot_test.cc"],
    deps = [
        ":depot",
        "//backend/depot",
        "//common:status_test_util",
        "//common:file_util",
        "//common:type",
        "@com_google_googletest//:gtest_main",
    ],
)

