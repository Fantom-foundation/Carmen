# Copyright (c) 2024 Fantom Foundation
#
# Use of this software is governed by the Business Source License included
# in the LICENSE file and at fantom.foundation/bsl11.
#
# Change Date: 2028-4-16
#
# On the date above, in accordance with the Business Source License, use of
# this software will be governed by the GNU Lesser General Public License v3.

cc_library(
    name = "btree",
    hdrs = ["btree.h"],
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

cc_library(
    name = "btree_map",
    hdrs = ["btree_map.h"],
    deps = [
        ":btree",
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
    name = "btree_map_test",
    srcs = ["btree_map_test.cc"],
    deps = [
        ":btree_map",
        ":test_util",
        "//common:file_util",
        "//common:status_test_util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "btree_set",
    hdrs = ["btree_set.h"],
    deps = [
        ":btree",
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
        ":test_util",
        "//common:file_util",
        "//common:status_test_util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_binary(
    name = "btree_set_benchmark",
    testonly = True,
    srcs = ["btree_set_benchmark.cc"],
    deps = [
        ":btree_set",
        "//backend/common:access_pattern",
        "//common:file_util",
        "//common:status_test_util",
        "//third_party/gperftools:profiler",
        "@com_github_google_benchmark//:benchmark_main",
    ],
)

cc_library(
    name = "entry",
    hdrs = ["entry.h"],
    deps = [
        "//common:type",
        "@com_google_absl//absl/base",
    ],
)

cc_test(
    name = "entry_test",
    srcs = ["entry_test.cc"],
    deps = [
        ":entry",
        "//common:type",
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
        ":entry",
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
        ":entry",
        ":nodes",
        "//backend/common:page_manager",
        "//common:status_test_util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "test_util",
    testonly = True,
    srcs = ["test_util.cc"],
    hdrs = ["test_util.h"],
)
