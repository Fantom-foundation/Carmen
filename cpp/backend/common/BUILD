
cc_library(
    name = "page_id",
    hdrs = ["page_id.h"],
    visibility = ["//backend:__subpackages__"],
)

cc_library(
    name = "file",
    hdrs = ["file.h"],
    srcs = ["file.cc"],
    deps = [":page_id"],
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