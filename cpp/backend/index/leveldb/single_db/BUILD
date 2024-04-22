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
    srcs = ["index.cc"],
    hdrs = ["index.h"],
    visibility = ["//visibility:public"],
    deps = [
        "//backend/common/leveldb",
        "//backend/index/leveldb:index",
        "//common:hash",
        "//common:status_util",
        "//common:type",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_test(
    name = "index_test",
    srcs = ["index_test.cc"],
    deps = [
        ":index",
        "//backend/index",
        "//backend/index:index_test_suite",
        "//common:file_util",
        "//common:status_test_util",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_binary(
    name = "index_benchmark",
    testonly = True,
    srcs = ["index_benchmark.cc"],
    deps = [
        "//backend/index/leveldb/single_db:index",
        "//common:type",
        "//third_party/gperftools:profiler",
        "@com_github_google_benchmark//:benchmark_main",
    ],
)
