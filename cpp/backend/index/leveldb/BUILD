cc_library(
    name = "index",
    hdrs = ["index.h"],
    srcs = ["index.cc"],
    deps = [
        "//common:type",
        "//common:hash",
        "@com_github_google_leveldb//:leveldb",
    ],
    visibility = ["//visibility:public"],
)

cc_test(
    name = "index_test",
    srcs = ["index_test.cc"],
    deps = [
        ":index",
        "@com_google_googletest//:gtest_main",
        "@com_github_google_leveldb//:leveldb",
    ],
)
