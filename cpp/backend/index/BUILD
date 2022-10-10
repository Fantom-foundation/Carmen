cc_library(
    name = "index",
    hdrs = ["index.h"],
    visibility = ["//visibility:public"],
)

cc_test(
    name = "index_test",
    srcs = ["index_test.cc"],
    deps = [
        "//backend/index/memory:index",
        "//common:test_util",
        "//common:type", 
        "@com_google_googletest//:gtest_main",
    ],
)