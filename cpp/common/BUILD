cc_library(
    name="type",
    hdrs=["type.h"],
    srcs=["type.cc"],
    deps=[
        ":hex_util"
    ],
)

cc_library(
    name="hex_util",
    hdrs=["hex_util.h"],
    srcs=["hex_util.cc"],
    deps=[],
)

cc_test(
    name = "type_test",
    srcs = ["type_test.cc"],
    deps = [
        ":type",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "hex_util_test",
    srcs = ["hex_util_test.cc"],
    deps = [
        ":hex_util",
        ":type",
        "@com_google_googletest//:gtest_main",
    ],
)
