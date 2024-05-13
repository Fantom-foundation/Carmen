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
    name = "sqlite",
    srcs = ["sqlite.cc"],
    hdrs = ["sqlite.h"],
    visibility = ["//visibility:public"],
    deps = [
        "//common:memory_usage",
        "//common:status_util",
        "//common:type",
        "@com_github_rockwotj_sqlite_bazel//:sqlite3",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_test(
    name = "sqlite_test",
    srcs = ["sqlite_test.cc"],
    deps = [
        ":sqlite",
        "//common:file_util",
        "//common:memory_usage",
        "//common:status_test_util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_binary(
    name = "sqlite_benchmark",
    testonly = True,
    srcs = ["sqlite_benchmark.cc"],
    deps = [
        ":sqlite",
        "//backend/common:access_pattern",
        "//common:file_util",
        "//common:status_test_util",
        "//third_party/gperftools:profiler",
        "@com_github_google_benchmark//:benchmark_main",
    ],
)
