cc_library(
    name = "hash_page",
    hdrs = ["hash_page.h"],
    deps = [
        "//backend/common:page_id",
        "//common:type",
    ],
)

cc_test(
    name = "hash_page_test",
    srcs = ["hash_page_test.cc"],
    deps = [
        ":hash_page", 
        "//backend/common:page",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "stable_hash",
    hdrs = ["stable_hash.h"],
    deps = [
        "@com_google_absl//absl/hash",
        "@com_google_absl//absl/numeric:int128",
    ],
)

cc_test(
    name = "stable_hash_test",
    srcs = ["stable_hash_test.cc"],
    deps = [
        ":stable_hash", 
        "//backend/common:page",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/hash",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_binary(
    name = "stable_hash_benchmark",
    srcs = ["stable_hash_benchmark.cc"],
    deps = [
        ":stable_hash",
        "//common:type",
        "@com_google_absl//absl/hash",
        "@com_github_google_benchmark//:benchmark_main",
        "//third_party/gperftools:profiler",
    ],
)

cc_library(
    name = "index",
    hdrs = ["index.h"],
    deps = [
        ":hash_page",
        ":stable_hash",
        "//backend/common:file",
        "//backend/common:page_pool", 
        "//backend:structure",
        "//common:type",
        "//common:hash",
        "//common:status_util",
        "//common:memory_usage",
        "@com_google_absl//absl/status:status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:strings",
    ],
    visibility = ["//visibility:public"],
)

cc_test(
    name = "index_test",
    srcs = ["index_test.cc"],
    deps = [
        ":index", 
        "//backend/index:test_util",
        "//backend:structure",
        "//common:status_test_util",
        "@com_google_googletest//:gtest_main",
    ],
)
