cc_library(
    name = "page",
    hdrs = ["page.h"],
    deps = [
        "//common:type",
        "//common:hash",
    ],
)

cc_test(
    name = "page_test",
    srcs = ["page_test.cc"],
    deps = [
        ":page",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "file",
    hdrs = ["file.h"],
    srcs = ["file.cc"],
    deps = [
        ":page",
    ]
)

cc_test(
    name = "file_test",
    srcs = ["file_test.cc"],
    deps = [
        ":file",
        "//common:file_util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "page_pool",
    hdrs = ["page_pool.h"],
    deps = [
        ":page",
        ":file",
        "@com_google_absl//absl/container:flat_hash_map",
    ]
)

cc_test(
    name = "page_pool_test",
    srcs = ["page_pool_test.cc"],
    deps = [
        ":page_pool",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "hash_tree",
    hdrs = ["hash_tree.h"],
    srcs = ["hash_tree.cc"],
    deps = [
        ":page",
        "//common:type",
        "//common:hash",
        "@com_google_absl//absl/container:flat_hash_set",
    ]
)

cc_test(
    name = "hash_tree_test",
    srcs = ["hash_tree_test.cc"],
    deps = [
        ":hash_tree",
        "//common:type",
        "//common:hash",
        "//common:file_util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "store",
    hdrs = ["store.h"],
    deps = [
        ":file",
        ":page_pool",
        ":hash_tree",
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
        "@com_google_googletest//:gtest_main",
    ],
)
