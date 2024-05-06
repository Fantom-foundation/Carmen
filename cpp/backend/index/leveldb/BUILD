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
    name = "index",
    hdrs = ["index.h"],
    visibility = ["//backend/index/leveldb:__subpackages__"],
    deps = [
        "//backend:structure",
        "//backend/common/leveldb",
        "//common:hash",
        "//common:memory_usage",
        "//common:status_util",
        "//common:type",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_binary(
    name = "index_benchmark",
    testonly = True,
    srcs = ["index_benchmark.cc"],
    deps = [
        "//backend/index/leveldb/multi_db:index",
        "//backend/index/leveldb/single_db:index",
        "//common:file_util",
        "//common:status_test_util",
        "//common:type",
        "//third_party/gperftools:profiler",
        "@com_github_google_benchmark//:benchmark_main",
    ],
)
