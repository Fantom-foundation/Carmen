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
    name = "page_id",
    hdrs = ["page_id.h"],
    visibility = ["//backend:__subpackages__"],
)

cc_library(
    name = "page",
    hdrs = ["page.h"],
    visibility = ["//backend:__subpackages__"],
    deps = [
        "//common:hash",
        "//common:type",
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
    name = "access_pattern",
    hdrs = ["access_pattern.h"],
    visibility = ["//backend:__subpackages__"],
)

cc_library(
    name = "file",
    srcs = ["file.cc"],
    hdrs = ["file.h"],
    visibility = [
        "//archive:__subpackages__",
        "//backend:__subpackages__",
    ],
    deps = [
        ":page",
        ":page_id",
        "//common:fstream",
        "//common:status_util",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
    ],
)

cc_test(
    name = "file_test",
    srcs = ["file_test.cc"],
    deps = [
        ":file",
        "//common:file_util",
        "//common:status_test_util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_binary(
    name = "file_benchmark",
    testonly = True,
    srcs = ["file_benchmark.cc"],
    deps = [
        ":file",
        "//common:file_util",
        "//common:status_test_util",
        "//third_party/gperftools:profiler",
        "@com_github_google_benchmark//:benchmark_main",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_library(
    name = "eviction_policy",
    srcs = ["eviction_policy.cc"],
    hdrs = ["eviction_policy.h"],
    deps = [
        ":access_pattern",
        "@com_google_absl//absl/container:btree",
    ],
)

cc_test(
    name = "eviction_policy_test",
    srcs = ["eviction_policy_test.cc"],
    deps = [
        ":eviction_policy",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_binary(
    name = "eviction_policy_benchmark",
    srcs = ["eviction_policy_benchmark.cc"],
    deps = [
        ":access_pattern",
        ":eviction_policy",
        "//third_party/gperftools:profiler",
        "@com_github_google_benchmark//:benchmark_main",
    ],
)

cc_library(
    name = "page_pool",
    hdrs = ["page_pool.h"],
    visibility = ["//backend:__subpackages__"],
    deps = [
        ":eviction_policy",
        ":file",
        ":page_id",
        "//common:memory_usage",
        "//common:status_util",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_test(
    name = "page_pool_test",
    srcs = ["page_pool_test.cc"],
    deps = [
        ":page",
        ":page_pool",
        "//common:status_test_util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_binary(
    name = "page_pool_benchmark",
    testonly = True,
    srcs = ["page_pool_benchmark.cc"],
    deps = [
        ":access_pattern",
        ":eviction_policy",
        ":page_pool",
        "//common:status_test_util",
        "//third_party/gperftools:profiler",
        "@com_github_google_benchmark//:benchmark_main",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_library(
    name = "page_manager",
    hdrs = ["page_manager.h"],
    visibility = ["//backend:__subpackages__"],
    deps = [
        ":page",
        ":page_id",
        ":page_pool",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_test(
    name = "page_manager_test",
    testonly = True,
    srcs = ["page_manager_test.cc"],
    deps = [
        ":page_manager",
        "//common:status_test_util",
        "//third_party/gperftools:profiler",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
    ],
)
