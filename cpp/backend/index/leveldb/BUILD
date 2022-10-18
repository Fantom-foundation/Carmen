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

cc_library(
    name = "test_util",
    hdrs = ["test_util.h"],
    deps = [
        "//common:hash",
        "//common:file_util",
        "@com_google_absl//absl/status:statusor"
    ],
    visibility = ["//backend/index:__subpackages__"],
    testonly = True,
)


cc_test(
    name = "index_test",
    srcs = ["index_test.cc"],
    deps = [
        ":index",
        "//common:file_util",
        "//backend/index",
        "//backend/index:test_util",
        "//backend/index/leveldb:test_util",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/status:statusor"
    ],
)
