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
    name = "test_util",
    testonly = True,
    hdrs = ["test_util.h"],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "macro_utils",
    hdrs = ["macro_utils.h"],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "benchmark",
    hdrs = ["benchmark.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":macro_utils",
        "@com_github_google_benchmark//:benchmark",
        "@com_google_absl//absl/strings",
    ],
)

cc_binary(
    name = "benchmark_benchmark",
    srcs = ["benchmark_benchmark.cc"],
    deps = [
        ":benchmark",
        "//third_party/gperftools:profiler",
        "@com_github_google_benchmark//:benchmark_main",
    ],
)

cc_library(
    name = "hex_util",
    srcs = ["hex_util.cc"],
    hdrs = ["hex_util.h"],
    visibility = ["//visibility:public"],
    deps = [],
)

cc_test(
    name = "hex_util_test",
    srcs = ["hex_util_test.cc"],
    deps = [
        ":hex_util",
        ":type",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "type",
    hdrs = ["type.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":hex_util",
    ],
)

cc_test(
    name = "type_test",
    srcs = ["type_test.cc"],
    deps = [
        ":test_util",
        ":type",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "hash",
    srcs = ["hash.cc"],
    hdrs = ["hash.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":type",
        "@boringssl//:crypto",
        "@ethash//:keccak",
    ],
)

cc_test(
    name = "hash_test",
    srcs = ["hash_test.cc"],
    deps = [
        ":hash",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "heterogenous_map",
    hdrs = ["heterogenous_map.h"],
    visibility = ["//visibility:public"],
    deps = [
        "@com_google_absl//absl/container:flat_hash_map",
    ],
)

cc_test(
    name = "heterogenous_map_test",
    srcs = ["heterogenous_map_test.cc"],
    deps = [
        ":heterogenous_map",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "file_util",
    testonly = True,
    srcs = ["file_util.cc"],
    hdrs = ["file_util.h"],
    visibility = ["//visibility:public"],
    deps = [
    ],
)

cc_test(
    name = "file_util_test",
    srcs = ["file_util_test.cc"],
    deps = [
        ":file_util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "account_state",
    srcs = ["account_state.cc"],
    hdrs = ["account_state.h"],
    visibility = ["//visibility:public"],
    deps = [
    ],
)

cc_test(
    name = "account_state_test",
    srcs = ["account_state_test.cc"],
    deps = [
        ":account_state",
        ":test_util",
        ":type",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_binary(
    name = "hash_benchmark",
    srcs = ["hash_benchmark.cc"],
    deps = [
        ":hash",
        "//third_party/gperftools:profiler",
        "@com_github_google_benchmark//:benchmark_main",
    ],
)

cc_library(
    name = "memory_usage",
    srcs = ["memory_usage.cc"],
    hdrs = ["memory_usage.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":status_util",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/functional:function_ref",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
    ],
)

cc_test(
    name = "memory_usage_test",
    srcs = ["memory_usage_test.cc"],
    deps = [
        ":memory_usage",
        ":status_test_util",
        ":test_util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "status_util",
    hdrs = ["status_util.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":macro_utils",
        "@com_google_absl//absl/base",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_test(
    name = "status_util_test",
    srcs = ["status_util_test.cc"],
    deps = [
        ":status_test_util",
        ":status_util",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "status_test_util",
    testonly = True,
    hdrs = ["status_test_util.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":status_util",
        "@com_google_absl//absl/base",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest",
    ],
)

cc_test(
    name = "status_test_util_test",
    srcs = ["status_test_util_test.cc"],
    deps = [
        ":status_test_util",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "byte_util",
    hdrs = ["byte_util.h"],
    visibility = ["//visibility:public"],
    deps = [
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_test(
    name = "byte_util_test",
    srcs = ["byte_util_test.cc"],
    deps = [
        ":byte_util",
        "//common:status_test_util",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "fstream",
    srcs = ["fstream.cc"],
    hdrs = ["fstream.h"],
    visibility = ["//visibility:public"],
    deps = [
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
    ],
)

cc_test(
    name = "fstream_test",
    srcs = ["fstream_test.cc"],
    deps = [
        ":file_util",
        ":fstream",
        ":status_test_util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "variant_util",
    hdrs = ["variant_util.h"],
    visibility = ["//visibility:public"],
    deps = [],
)

cc_test(
    name = "variant_util_test",
    srcs = ["variant_util_test.cc"],
    deps = [
        ":variant_util",
        "@com_google_googletest//:gtest_main",
    ],
)
