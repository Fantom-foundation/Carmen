cc_library(
    name = "btree_set",
    hdrs = ["btree_set.h"],
    deps = [
        ":nodes",
        "//backend/common:page_manager",
        "//common:status_util",
        "//common:type",
        "//common:variant_util",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_test(
    name = "btree_set_test",
    srcs = ["btree_set_test.cc"],
    deps = [
        ":btree_set",
        "//common:file_util",
        "//common:status_test_util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "insert_result",
    hdrs = ["insert_result.h"],
    deps = [
        "//backend/common:page_id",
    ],
)

cc_library(
    name = "nodes",
    hdrs = ["nodes.h"],
    deps = [
        ":insert_result",
        "//backend/common:page",
        "//backend/common:page_id",
        "//backend/common:page_pool",
        "//common:type",
        "//common:variant_util",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
    ],
)

cc_test(
    name = "nodes_test",
    srcs = ["nodes_test.cc"],
    deps = [
        ":nodes",
        "//backend/common:page_manager",
        "//common:status_test_util",
        "@com_google_googletest//:gtest_main",
    ],
)
