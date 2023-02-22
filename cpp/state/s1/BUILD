cc_library(
    name = "state",
    hdrs = ["state.h"],
    visibility = [
        "//state:__subpackages__",
    ],
    deps = [
        "//archive",
        "//backend:structure",
        "//common:account_state",
        "//common:type",
        "//state:schema",
        "//state:update",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_test(
    name = "state_test",
    srcs = ["state_test.cc"],
    deps = [
        ":state",
        "//archive:test_util",
        "//archive/leveldb:archive",
        "//backend/depot:test_util",
        "//backend/depot/memory:depot",
        "//backend/index:test_util",
        "//backend/index/memory:index",
        "//backend/multimap:test_util",
        "//backend/multimap/memory:multimap",
        "//backend/store:test_util",
        "//backend/store/memory:store",
        "//common:account_state",
        "//common:status_test_util",
        "//common:type",
        "//state:state_test_suite",
        "//state:update",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
    ],
)
