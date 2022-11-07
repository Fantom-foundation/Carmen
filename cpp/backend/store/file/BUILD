cc_library(
    name = "store",
    hdrs = ["store.h"],
    deps = [
        "//backend/common:file",
        "//backend/common:page_pool",
        "//backend/store:hash_tree",
        "//common:type",
        "//common:hash",
    ],
    visibility = ["//visibility:public"],
)

cc_test(
    name = "store_test",
    srcs = ["store_test.cc"],
    deps = [
        ":store",
        "//common:file_util",
        "@com_google_googletest//:gtest_main",
    ],
)
