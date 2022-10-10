cc_library(
    name = "store_handler",
    hdrs = ["store_handler.h"],
    deps = [
        "//backend/store/memory:store",
        "//backend/store/file:store",
        "//common:file_util",
        "//common:type", 
    ],
    testonly = True,
)

cc_test(
    name = "store_test",
    srcs = ["store_test.cc"],
    deps = [
        ":store_handler",
        "//backend/store/memory:store",
        "//backend/store/file:store",
        "//common:test_util",
        "//common:type", 
        "@com_google_googletest//:gtest_main",
    ],
)

cc_binary(
    name = "store_benchmark",
    srcs = ["store_benchmark.cc"],
    deps = [
        ":store_handler",
        "//backend/store/memory:store",
        "//backend/store/file:store",
        "@com_github_google_benchmark//:benchmark_main",
        "//third_party/gperftools:profiler",
    ],
    testonly = True,
)