cc_library(
    name = "depot",
    hdrs = ["depot.h"],
    deps = [
        "//backend/store:hash_tree",
        "//common:hash",
        "//common:type",
        "//common:byte_util",
        "//common:status_util",
        "//backend/common/leveldb:level_db",
        "@com_google_absl//absl/status:status",
        "@com_google_absl//absl/status:statusor",
    ],
    visibility = ["//visibility:public"],
)
