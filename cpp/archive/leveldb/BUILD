cc_library(
    name = "archive",
    srcs = ["archive.cc"],
    hdrs = ["archive.h"],
    visibility = [
        "//state:__subpackages__",
        "//tools:__subpackages__",
    ],
    deps = [
        ":keys",
        "//backend/common/leveldb",
        "//common:byte_util",
        "//common:type",
        "//state:update",
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
        "//archive:archive_test_suite",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "keys",
    srcs = ["keys.cc"],
    hdrs = ["keys.h"],
    deps = [
        "//common:type",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_test(
    name = "keys_test",
    srcs = ["keys_test.cc"],
    deps = [
        ":keys",
        "//common:status_test_util",
        "//common:type",
        "@com_google_googletest//:gtest_main",
    ],
)
