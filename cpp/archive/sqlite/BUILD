cc_library(
    name = "archive",
    srcs = ["archive.cc"],
    hdrs = ["archive.h"],
    visibility = [
        "//state:__subpackages__",
        "//tools:__subpackages__",
    ],
    deps = [
        "//backend/common/sqlite",
        "//common:type",
        "//state:update",
        "@com_google_absl//absl/container:btree",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
    ],
)

cc_test(
    name = "archive_test",
    srcs = ["archive_test.cc"],
    deps = [
        ":archive",
        "//archive",
        "//archive:archive_test_suite",
        "//common:file_util",
        "//common:hash",
        "//common:status_test_util",
        "//common:type",
        "@com_google_googletest//:gtest_main",
    ],
)
