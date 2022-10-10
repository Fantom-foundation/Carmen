cc_library(
    name = "index",
    hdrs = ["index.h"],
    deps = [
        "//common:type",
    ],
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

cc_library(
    name = "test_util",
    hdrs = ["test_util.h"],
    deps = [
        "//common:hash",
        "@com_google_googletest//:gtest",
    ],
    visibility = ["//backend/index:__subpackages__"],
    testonly = True,
)

cc_test(
    name = "test_util_test",
    srcs = ["test_util_test.cc"],
    deps = [
        ":index",
        ":test_util",
        "@com_google_googletest//:gtest_main",
    ],
)