cc_library(
    name = "state",
    hdrs = ["state.h"],
    deps = [
        ":update",
        "//backend:structure",
        "//common:account_state",
        "//common:type",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_test(
    name = "state_test",
    srcs = ["state_test.cc"],
    deps = [
        ":configurations",
        ":state",
        "//common:account_state",
        "//common:file_util",
        "//common:status_test_util",
        "//common:type",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "configurations",
    hdrs = ["configurations.h"],
    deps = [
        ":state",
        "//backend/depot/file:depot",
        "//backend/depot/leveldb:depot",
        "//backend/depot/memory:depot",
        "//backend/index/cache",
        "//backend/index/file:index",
        "//backend/index/leveldb/multi_db:index",
        "//backend/index/memory:index",
        "//backend/multimap/memory:multimap",
        "//backend/store/file:store",
        "//backend/store/leveldb:store",
        "//backend/store/memory:store",
    ],
)

cc_library(
    name = "c_state",
    srcs = ["c_state.cc"],
    hdrs = ["c_state.h"],
    deps = [
        ":configurations",
        ":state",
        "@com_google_absl//absl/status",
    ],
    alwayslink = True,
)

cc_test(
    name = "c_state_test",
    srcs = ["c_state_test.cc"],
    deps = [
        ":c_state",
        "//common:account_state",
        "//common:file_util",
        "//common:type",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_binary(
    name = "state_benchmark",
    testonly = True,
    srcs = ["state_benchmark.cc"],
    deps = [
        ":configurations",
        "//common:benchmark",
        "//common:file_util",
        "//common:status_test_util",
        "//third_party/gperftools:profiler",
        "@com_github_google_benchmark//:benchmark_main",
    ],
)

cc_binary(
    name = "libcarmen.so",
    linkopts = select({
        "@platforms//os:osx": [
            # Needed to make resulting library moveable
            "-install_name @rpath/libcarmen.so",
        ],
        "//conditions:default": [],
    }),
    linkshared = True,
    deps = [
        ":c_state",
    ],
)

cc_library(
    name = "update",
    srcs = ["update.cc"],
    hdrs = ["update.h"],
    deps = [
        "//common:hash",
        "//common:status_util",
        "//common:type",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
    ],
)

cc_test(
    name = "update_test",
    srcs = ["update_test.cc"],
    deps = [
        ":update",
        "//common:hash",
        "//common:status_test_util",
        "//common:test_util",
        "//common:type",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "archive",
    srcs = ["archive.cc"],
    hdrs = ["archive.h"],
    deps = [
        ":update",
        "//backend/common/sqlite",
        "//common:type",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
    ],
)

cc_test(
    name = "archive_test",
    srcs = ["archive_test.cc"],
    deps = [
        ":archive",
        "//common:file_util",
        "//common:status_test_util",
        "//common:type",
        "@com_google_googletest//:gtest_main",
    ],
)
