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
    name = "depot",
    hdrs = ["depot.h"],
    visibility = ["//visibility:public"],
    deps = [
        "//backend:structure",
        "//backend/store:hash_tree",
        "//common:hash",
        "//common:type",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_test(
    name = "depot_test",
    srcs = ["depot_test.cc"],
    deps = [
        ":depot",
        "//backend/depot",
        "//backend/depot:depot_test_suite",
        "@com_google_googletest//:gtest_main",
    ],
)
