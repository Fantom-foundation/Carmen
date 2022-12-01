cc_library(
    name = "depot",
    hdrs = ["depot.h"],
    deps = [
        "//backend/store:hash_tree",
        "//backend:structure",
        "//common:hash",
        "//common:type",
        "@com_google_absl//absl/status:status",
        "@com_google_absl//absl/status:statusor",
    ],
    visibility = ["//visibility:public"],
)

cc_test(
    name = "depot_test",
    srcs = ["depot_test.cc"],
    deps = [
        ":depot",
        "//backend/depot",
        "@com_google_googletest//:gtest_main",
    ],
)