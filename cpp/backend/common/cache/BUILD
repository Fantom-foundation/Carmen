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
    name = "lru_cache",
    hdrs = ["lru_cache.h"],
    visibility = ["//backend:__subpackages__"],
    deps = [
        "//common:memory_usage",
        "@com_google_absl//absl/container:flat_hash_map",
    ],
)

cc_test(
    name = "lru_cache_test",
    srcs = ["lru_cache_test.cc"],
    deps = [
        ":lru_cache",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_binary(
    name = "lru_cache_benchmark",
    testonly = True,
    srcs = ["lru_cache_benchmark.cc"],
    deps = [
        ":lru_cache",
        "//common:benchmark",
        "//third_party/gperftools:profiler",
        "@com_github_google_benchmark//:benchmark_main",
    ],
)
