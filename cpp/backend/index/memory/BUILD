cc_library(
    name = "index",
    hdrs = ["index.h"],
    deps = [
        "//common:type",
        "//common:hash",
        "@com_google_absl//absl/container:flat_hash_map",
    ],
    visibility = ["//visibility:public"],
)

cc_test(
    name = "index_test",
    srcs = ["index_test.cc"],
    deps = [
        ":index", 
        "//backend/index",
        "//backend/index:test_util",
        "//backend/index:index_handler",
        "@com_google_googletest//:gtest_main",
    ],
)
