
cc_library(
    name = "page_id",
    hdrs = ["page_id.h"],
    visibility = ["//backend:__subpackages__"],
)
cc_library(
    name = "page",
    hdrs = ["page.h"],
    deps = [
        "//common:type",
        "//common:hash",
    ],
    visibility = ["//backend:__subpackages__"],
)

cc_test(
    name = "page_test",
    srcs = ["page_test.cc"],
    deps = [
        ":page",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "file",
    hdrs = ["file.h"],
    srcs = ["file.cc"],
    deps = [
        ":page_id",
        ":page",
    ],
    visibility = ["//backend:__subpackages__"],
)

cc_test(
    name = "file_test",
    srcs = ["file_test.cc"],
    deps = [
        ":file",
        "//common:file_util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_binary(
    name = "file_benchmark",
    srcs = ["file_benchmark.cc"],
    deps = [
        ":file",
        "//common:file_util",
        "@com_github_google_benchmark//:benchmark_main",
        "//third_party/gperftools:profiler",
    ],
    testonly = True,
)

cc_library(
    name = "page_pool",
    hdrs = ["page_pool.h"],
    deps = [
        ":file",
        ":page_id",
        "//common:memory_usage",
        "@com_google_absl//absl/container:flat_hash_map",
    ],
    visibility = ["//backend:__subpackages__"],
)

cc_test(
    name = "page_pool_test",
    srcs = ["page_pool_test.cc"],
    deps = [
        ":page",
        ":page_pool",
        "@com_google_googletest//:gtest_main",
    ],
)
