cc_library(
    name = "store",
    hdrs = ["store.h"],
    visibility = ["//visibility:public"],
    deps = [
        "//backend:structure",
        "//backend/common:file",
        "//backend/common:page_pool",
        "//backend/store:hash_tree",
        "//common:hash",
        "//common:status_util",
        "//common:type",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
    ],
)

cc_test(
    name = "store_test",
    srcs = ["store_test.cc"],
    deps = [
        ":store",
        "//backend:structure",
        "//common:file_util",
        "//common:status_test_util",
        "@com_google_googletest//:gtest_main",
    ],
)
