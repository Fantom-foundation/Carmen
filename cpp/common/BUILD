cc_library(
    name="hex_util",
    hdrs=["hex_util.h"],
    srcs=["hex_util.cc"],
    deps=[],
    visibility = ["//visibility:public"],
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

cc_library(
    name="type",
    hdrs=["type.h"],
    srcs=["type.cc"],
    deps=[
        ":hex_util"
    ],
    visibility = ["//visibility:public"],
)

cc_test(
    name = "type_test",
    srcs = ["type_test.cc"],
    deps = [
        ":type",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name="hash",
    hdrs=["hash.h"],
    srcs=["hash.cc"],
    deps=[
        ":type",
        "@boringssl//:crypto",
    ],
    visibility = ["//visibility:public"],
)

cc_test(
    name = "hash_test",
    srcs = ["hash_test.cc"],
    deps = [
        ":hash",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_googletest//:gtest_main",
    ],
)
