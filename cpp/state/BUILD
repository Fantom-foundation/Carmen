cc_library(
    name = "state",
    hdrs = ["state.h"],
    deps = [
        ":schema",
        ":update",
        "//archive",
        "//backend:structure",
        "//common:account_state",
        "//common:type",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_library(
    name = "state_test_suite",
    testonly = True,
    hdrs = ["state_test_suite.h"],
    visibility = [
        "//state:__subpackages__",
    ],
    deps = [
        ":state",
        ":update",
        "//archive:test_util",
        "//archive/leveldb:archive",
        "//backend/depot:test_util",
        "//backend/index:test_util",
        "//backend/multimap:test_util",
        "//backend/store:test_util",
        "//common:account_state",
        "//common:file_util",
        "//common:status_test_util",
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
        ":state_test_suite",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "schema",
    srcs = ["schema.cc"],
    hdrs = ["schema.h"],
    visibility = [
        "//state:__subpackages__",
    ],
)

cc_test(
    name = "schema_test",
    srcs = ["schema_test.cc"],
    deps = [
        ":schema",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "configurations",
    hdrs = ["configurations.h"],
    visibility = [
        "//state:__subpackages__",
    ],
    deps = [
        "//archive",
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
        "//archive",
        "//archive/leveldb:archive",
        "//archive/sqlite:archive",
        "//state/s1:state",
        "//state/s2:state",
        "//state/s3:state",
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
        "//common:status_test_util",
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
        "//archive/leveldb:archive",
        "//common:benchmark",
        "//common:file_util",
        "//common:status_test_util",
        "//state/s1:state",
        "//state/s2:state",
        "//state/s3:state",
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
    visibility = [
        "//archive:__subpackages__",
        "//state:__subpackages__",
    ],
    deps = [
        "//common:hash",
        "//common:status_util",
        "//common:type",
        "@com_google_absl//absl/container:flat_hash_map",
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
