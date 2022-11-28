cc_library(
    name = "index",
    hdrs = ["index.h"],
    deps = [
        "//backend/common/leveldb:leveldb",
        "//backend/index/leveldb:index",
    ],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "test_util",
    hdrs = ["test_util.h"],
    deps = [
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
        "//common:status_test_util",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/status:statusor"
    ],
)
