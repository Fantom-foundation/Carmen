# Copyright (c) 2024 Fantom Foundation
# 
# Use of this software is governed by the Business Source License included
# in the LICENSE file and at fantom.foundation/bsl11.
# 
# Change Date: 2028-4-16
# 
# On the date above, in accordance with the Business Source License, use
# of this software will be governed by the GNU Lesser General Public Licence v3.

cc_library(
    name = "index",
    hdrs = ["index.h"],
    visibility = ["//visibility:public"],
    deps = [
        "//backend:structure",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_test(
    name = "index_test",
    srcs = ["index_test.cc"],
    deps = [
        "//backend/common:file",
        "//backend/index/memory:index",
        "//common:status_test_util",
        "//common:test_util",
        "//common:type",
        "@com_google_googletest//:gtest_main",
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
        "//common:type",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest",
    ],
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

cc_library(
    name = "index_handler",
    testonly = True,
    hdrs = ["index_handler.h"],
    visibility = ["//backend/index:__subpackages__"],
    deps = [
        ":index",
        "//backend:structure",
        "//backend/index/cache",
        "//backend/index/file:index",
        "//backend/index/leveldb/multi_db:index",
        "//backend/index/leveldb/single_db:index",
        "//common:file_util",
        "//common:type",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_binary(
    name = "index_benchmark",
    testonly = True,
    srcs = ["index_benchmark.cc"],
    deps = [
        ":index_handler",
        "//backend/common:file",
        "//backend/index/cache",
        "//backend/index/file:index",
        "//backend/index/memory:index",
        "//backend/index/memory:linear_hash_index",
        "//common:benchmark",
        "//common:status_test_util",
        "//third_party/gperftools:profiler",
        "@com_github_google_benchmark//:benchmark_main",
    ],
)

cc_library(
    name = "index_test_suite",
    testonly = True,
    hdrs = ["index_test_suite.h"],
    visibility = ["//backend/index:__subpackages__"],
    deps = [
        ":index",
        "//backend/index:index_handler",
        "//backend/index/memory:index",
        "//common:hash",
        "//common:status_test_util",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest",
    ],
)
