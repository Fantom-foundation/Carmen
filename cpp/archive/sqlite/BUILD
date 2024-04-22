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
    name = "archive",
    srcs = ["archive.cc"],
    hdrs = ["archive.h"],
    visibility = [
        "//state:__subpackages__",
        "//tools:__subpackages__",
    ],
    deps = [
        "//backend/common:file",
        "//backend/common/sqlite",
        "//common:type",
        "//state:update",
        "@com_google_absl//absl/container:btree",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
    ],
)

cc_test(
    name = "archive_test",
    srcs = ["archive_test.cc"],
    deps = [
        ":archive",
        "//archive",
        "//archive:archive_test_suite",
        "//common:file_util",
        "//common:hash",
        "//common:status_test_util",
        "//common:type",
        "@com_google_googletest//:gtest_main",
    ],
)
