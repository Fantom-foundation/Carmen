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
    name = "multimap",
    hdrs = ["multimap.h"],
    visibility = ["//visibility:public"],
    deps = [
        "//backend/common:file",
        "//common:fstream",
        "//common:type",
        "@com_google_absl//absl/container:btree",
        "@com_google_absl//absl/strings",
    ],
)

cc_test(
    name = "multimap_test",
    srcs = ["multimap_test.cc"],
    deps = [
        ":multimap",
        "//backend/multimap",
        "//common:file_util",
        "//common:status_test_util",
        "//common:type",
        "@com_google_googletest//:gtest_main",
    ],
)
