cc_library(
    name = "level_db",
    hdrs = ["level_db.h"],
    srcs = ["level_db.cc"],
    deps = [
        "@com_github_google_leveldb//:leveldb",
        "@com_google_absl//absl/status:statusor"
    ],
    visibility = ["//backend/index/leveldb:__subpackages__"],
)

cc_test(
    name = "level_db_test",
    srcs = ["level_db_test.cc"],
    deps = [
        ":level_db",
        "//common:file_util",
        "//common:status_test_util",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/status:statusor"
    ],
)
