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
    name = "cache",
    hdrs = ["cache.h"],
    visibility = ["//visibility:public"],
    deps = [
        "//backend/common/cache:lru_cache",
        "//backend/depot",
        "//common:memory_usage",
        "//common:status_util",
        "//common:type",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_test(
    name = "cache_test",
    srcs = ["cache_test.cc"],
    deps = [
        ":cache",
        "//backend/depot:depot_test_suite",
        "//backend/depot:test_util",
        "//common:status_test_util",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
    ],
)
