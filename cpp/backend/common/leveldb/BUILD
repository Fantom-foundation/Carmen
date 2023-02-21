cc_library(
    name = "leveldb",
    srcs = ["leveldb.cc"],
    hdrs = ["leveldb.h"],
    visibility = [
        "//archive:__subpackages__",
        "//backend:__subpackages__",
    ],
    deps = [
        "//common:memory_usage",
        "//common:status_util",
        "@com_github_google_leveldb//:leveldb",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_test(
    name = "leveldb_test",
    srcs = ["leveldb_test.cc"],
    deps = [
        ":leveldb",
        "//common:file_util",
        "//common:status_test_util",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
    ],
)
