cc_library(
    name = "sqlite",
    srcs = ["sqlite.cc"],
    hdrs = ["sqlite.h"],
    visibility = ["//backend:__subpackages__"],
    deps = [
        "//common:status_util",
        "@com_github_rockwotj_sqlite_bazel//:sqlite3",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_test(
    name = "sqlite_test",
    srcs = ["sqlite_test.cc"],
    deps = [
        ":sqlite",
        "//common:file_util",
        "//common:status_test_util",
        "@com_google_googletest//:gtest_main",
    ],
)
