cc_library(
    name = "test_util",
    hdrs = ["test_util.h"],
    testonly = True,
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
    deps = [
        ":macro_utils",
        "@com_google_absl//absl/strings",
        "@com_github_google_benchmark//:benchmark",
    ],
    visibility = ["//visibility:public"],
)

cc_binary(
    name = "benchmark_benchmark",
    srcs = ["benchmark_benchmark.cc"],
    deps = [
        ":benchmark",
        "@com_github_google_benchmark//:benchmark_main",
        "//third_party/gperftools:profiler",
    ],
)

cc_library(
    name="hex_util",
    hdrs=["hex_util.h"],
    srcs=["hex_util.cc"],
    deps=[],
    visibility = ["//visibility:public"],
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
    name="type",
    hdrs=["type.h"],
    srcs=["type.cc"],
    deps=[
        ":hex_util"
    ],
    visibility = ["//visibility:public"],
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
    name="hash",
    hdrs=["hash.h"],
    srcs=["hash.cc"],
    deps=[
        ":type",
        "@boringssl//:crypto",
        "@ethash//:keccak",
    ],
    visibility = ["//visibility:public"],
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
    name="file_util",
    hdrs=["file_util.h"],
    srcs=["file_util.cc"],
    deps=[
    ],
    visibility = ["//visibility:public"],
    testonly = True,
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
    name="account_state",
    hdrs=["account_state.h"],
    srcs=["account_state.cc"],
    deps=[
    ],
    visibility = ["//visibility:public"],
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
        "@com_github_google_benchmark//:benchmark_main",
        "//third_party/gperftools:profiler",
    ],
)

cc_library(
    name="memory_usage",
    hdrs=["memory_usage.h"],
    srcs=["memory_usage.cc"],
    deps=[
        ":status_util",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/functional:function_ref",
    ],
    visibility = ["//visibility:public"],
)

cc_test(
    name = "memory_usage_test",
    srcs = ["memory_usage_test.cc"],
    deps = [
        ":memory_usage",
        ":test_util",
        ":status_test_util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name="status_util",
    hdrs=["status_util.h"],
    deps=[
        "@com_google_absl//absl/base",
        "@com_google_absl//absl/status:status",
        "@com_google_absl//absl/status:statusor"
    ],
    visibility = ["//visibility:public"],
)

cc_test(
    name = "status_util_test",
    srcs = ["status_util_test.cc"],
    deps = [
        ":status_util",
        ":status_test_util",
        "@com_google_absl//absl/status:status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name="status_test_util",
    hdrs=["status_test_util.h"],
    deps=[
        ":status_util",
        "@com_google_absl//absl/base",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/status:status",
        "@com_google_absl//absl/status:statusor"
    ],
    visibility = ["//visibility:public"],
    testonly = True,
)

cc_test(
    name = "status_test_util_test",
    srcs = ["status_test_util_test.cc"],
    deps = [
        ":status_test_util",
        "@com_google_absl//absl/status:status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name="byte_util",
    hdrs=["byte_util.h"],
    deps=[
        "@com_google_absl//absl/status:statusor"
    ],
    visibility = ["//visibility:public"],
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
