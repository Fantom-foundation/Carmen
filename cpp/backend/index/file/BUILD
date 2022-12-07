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
    name = "index",
    hdrs = ["index.h"],
    deps = [
        ":hash_page",
        "//backend/common:file",
        "//backend/common:page_pool", 
        "//backend:structure",
        "//common:type",
        "//common:hash",
        "//common:status_util",
        "//common:memory_usage",
        "@com_google_absl//absl/status:status",
        "@com_google_absl//absl/status:statusor",
    ],
    visibility = ["//visibility:public"],
)

cc_test(
    name = "index_test",
    srcs = ["index_test.cc"],
    deps = [
        ":index", 
        "//backend/index:test_util",
        "//common:status_test_util",
        "@com_google_googletest//:gtest_main",
    ],
)
