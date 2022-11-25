cc_library(
    name = "leveldb",
    hdrs = ["leveldb.h"],
    srcs = ["leveldb.cc"],
    deps = [
        "//common:status_util",
        "//common:memory_usage",
        "@com_github_google_leveldb//:leveldb",
        "@com_google_absl//absl/status:statusor"
    ],
    visibility = ["//backend:__subpackages__"],
)

cc_test(
    name = "leveldb_test",
    srcs = ["leveldb_test.cc"],
    deps = [
        ":leveldb",
        "//common:file_util",
        "//common:status_test_util",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/status:statusor"
    ],
)
