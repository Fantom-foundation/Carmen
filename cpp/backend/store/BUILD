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
    name = "store",
    hdrs = ["store.h"],
    visibility = ["//visibility:public"],
    deps = [
        "//backend:structure",
        "//backend/common:page_id",
        "//common:status_util",
        "@com_google_absl//absl/status",
    ],
)

cc_library(
    name = "store_handler",
    testonly = True,
    hdrs = ["store_handler.h"],
    deps = [
        "//backend/store/file:store",
        "//backend/store/memory:store",
        "//common:file_util",
        "//common:status_util",
        "//common:type",
    ],
)

cc_library(
    name = "test_util",
    testonly = True,
    hdrs = ["test_util.h"],
    visibility = ["//visibility:public"],
    deps = [
        "//backend:structure",
        "//common:memory_usage",
        "//common:status_util",
        "//common:type",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest",
    ],
)

cc_library(
    name = "store_test_suite",
    testonly = True,
    srcs = ["store_test_suite.h"],
    visibility = ["//backend/store:__subpackages__"],
    deps = [
        ":store",
        ":store_handler",
        "//common:hash",
        "//common:status_test_util",
        "//common:test_util",
        "//common:type",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_binary(
    name = "store_benchmark",
    testonly = True,
    srcs = ["store_benchmark.cc"],
    deps = [
        ":store_handler",
        "//backend/store/file:store",
        "//backend/store/leveldb:store",
        "//backend/store/memory:store",
        "//common:benchmark",
        "//common:status_test_util",
        "//third_party/gperftools:profiler",
        "@com_github_google_benchmark//:benchmark_main",
    ],
)

cc_library(
    name = "hash_tree",
    srcs = ["hash_tree.cc"],
    hdrs = ["hash_tree.h"],
    visibility = ["//backend:__subpackages__"],
    deps = [
        "//backend/common:page",
        "//backend/common:page_id",
        "//backend/common/leveldb",
        "//common:byte_util",
        "//common:fstream",
        "//common:hash",
        "//common:memory_usage",
        "//common:status_util",
        "//common:type",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_test(
    name = "hash_tree_test",
    srcs = ["hash_tree_test.cc"],
    deps = [
        ":hash_tree",
        "//common:file_util",
        "//common:hash",
        "//common:status_test_util",
        "//common:type",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
    ],
)
