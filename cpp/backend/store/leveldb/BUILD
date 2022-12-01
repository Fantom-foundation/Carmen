cc_library(
    name = "store",
    hdrs = ["store.h"],
    deps = [
        "//backend/store",
        "//backend/store:hash_tree",
        "//common:type",
        "//common:hash",
        "//backend/common/leveldb:leveldb",
        "//common:status_util",
        "//common:byte_util",
        "@com_google_absl//absl/status:status",
        "@com_google_absl//absl/status:statusor",
    ],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "test_util",
    hdrs = ["test_util.h"],
    deps = [
        ":store",
        "//backend/store",
        "//common:hash",
        "//common:type",
        "//common:memory_usage",
        "//common:status_util",
        "@com_google_absl//absl/status:statusor",
    ],
    visibility = ["//visibility:public"],
)
