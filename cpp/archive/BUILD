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
    name = "archive",
    hdrs = ["archive.h"],
    visibility = ["//visibility:public"],
    deps = [
        "//backend:structure",
        "//common:type",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_library(
    name = "test_util",
    testonly = True,
    hdrs = ["test_util.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":archive",
        "//common:memory_usage",
        "//common:type",
        "//state:update",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest",
    ],
)

cc_library(
    name = "archive_test_suite",
    testonly = True,
    hdrs = ["archive_test_suite.h"],
    visibility = ["//archive:__subpackages__"],
    deps = [
        ":archive",
        "//common:file_util",
        "//common:hash",
        "//common:status_test_util",
        "//common:type",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest",
    ],
)
