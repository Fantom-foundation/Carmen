cc_library(
    name = "index",
    hdrs = ["index.h"],
    srcs = ["index.cc"],
    deps = [
        "//common:type",
        "//common:hash",
        "@com_github_google_leveldb//:leveldb",
        "@com_google_absl//absl/status:statusor"
    ],
    visibility = ["//visibility:public"],
)

cc_test(
    name = "index_test",
    srcs = ["index_test.cc"],
    deps = [
        ":index",
        "//common:file_util",
        "//common:hash",
        "//common:type",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/status:statusor",
        "@com_github_google_leveldb//:leveldb",
    ],
)