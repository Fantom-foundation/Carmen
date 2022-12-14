cc_library(
    name = "store",
    hdrs = ["store.h"],
    deps = [
        "//backend/store",
        "//backend/store:hash_tree",
        "//common:type",
        "//common:hash",
        "//backend/common/leveldb:leveldb",
        "//common:status_util",
        "//common:byte_util",
        "@com_google_absl//absl/status:status",
        "@com_google_absl//absl/status:statusor",
    ],
    visibility = ["//visibility:public"],
)

cc_test(
    name = "store_test",
    srcs = ["store_test.cc"],
    deps = [
        ":store",
        "//common:file_util",
        "//common:status_test_util",
        "//backend:structure",
        "@com_google_googletest//:gtest_main",
    ],
)
