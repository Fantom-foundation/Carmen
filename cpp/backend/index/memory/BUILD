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
        "@com_google_googletest//:gtest_main",
    ],
)