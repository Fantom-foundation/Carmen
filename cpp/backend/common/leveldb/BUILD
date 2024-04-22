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
    name = "leveldb",
    srcs = ["leveldb.cc"],
    hdrs = ["leveldb.h"],
    visibility = [
        "//archive:__subpackages__",
        "//backend:__subpackages__",
    ],
    deps = [
        "//backend/common:file",
        "//common:memory_usage",
        "//common:status_util",
        "@com_github_google_leveldb//:leveldb",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_test(
    name = "leveldb_test",
    srcs = ["leveldb_test.cc"],
    deps = [
        ":leveldb",
        "//common:file_util",
        "//common:status_test_util",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
    ],
)
