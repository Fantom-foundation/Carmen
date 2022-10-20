cc_library(
    name = "ldb_instance",
    hdrs = ["ldb_instance.h"],
    srcs = ["ldb_instance.cc"],
    deps = [
        "@com_github_google_leveldb//:leveldb",
        "@com_google_absl//absl/status:statusor"
    ],
    visibility = ["//backend/index/leveldb:__subpackages__"],
)

cc_test(
    name = "ldb_interface_test",
    srcs = ["ldb_instance_test.cc"],
    deps = [
        ":ldb_instance",
        "//common:file_util",
        "//common:status_test_util",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/status:statusor"
    ],
)
