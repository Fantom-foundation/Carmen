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
    name = "index",
    hdrs = ["index.h"],
    visibility = ["//visibility:public"],
    deps = [
        "//backend:structure",
        "//backend/index",
        "//common:hash",
        "//common:memory_usage",
        "//common:type",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_test(
    name = "index_test",
    srcs = ["index_test.cc"],
    deps = [
        ":index",
        "//backend/index",
        "//backend/index:index_test_suite",
        "//common:status_test_util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "linear_hash_index",
    hdrs = ["linear_hash_index.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":linear_hash_map",
        "//backend:structure",
        "//common:hash",
        "//common:memory_usage",
        "//common:type",
        "@com_google_absl//absl/hash",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_test(
    name = "linear_hash_index_test",
    srcs = ["linear_hash_index_test.cc"],
    deps = [
        ":linear_hash_index",
        "//backend/index",
        "//backend/index:index_test_suite",
        "//common:status_test_util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "linear_hash_map",
    hdrs = ["linear_hash_map.h"],
    deps = [
        "//common:memory_usage",
        "@com_google_absl//absl/hash",
    ],
)

cc_test(
    name = "linear_hash_map_test",
    srcs = ["linear_hash_map_test.cc"],
    deps = [
        ":linear_hash_map",
        "@com_google_googletest//:gtest_main",
    ],
)
