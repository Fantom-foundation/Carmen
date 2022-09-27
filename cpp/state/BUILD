cc_library(
    name = "state",
    hdrs = ["state.h"],
)

cc_test(
    name = "state_test",
    srcs = ["state_test.cc"],
    deps = [
        ":state",
        "//backend/index/memory:index",
        "//backend/store/memory:store",
        "@com_google_googletest//:gtest_main",
    ],
)