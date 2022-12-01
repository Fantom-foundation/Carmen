cc_library(
    name = "store",
    hdrs = ["store.h"],
    deps = [
        "//backend/store",
        "//backend/store:hash_tree",
        "//common:type",
        "//common:hash",
        "//common:memory_usage",
        "@com_google_absl//absl/status:statusor",
    ],
    visibility = ["//visibility:public"],
)

cc_test(
    name = "store_test",
    srcs = ["store_test.cc"],
    deps = [
        ":store", 
        "@com_google_googletest//:gtest_main",
    ],
)