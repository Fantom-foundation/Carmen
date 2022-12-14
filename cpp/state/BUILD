cc_library(
    name = "state",
    hdrs = ["state.h"],
    deps = [
        "//backend:structure",
        "//common:account_state",
        "//common:type",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/status:status",
    ]
)

cc_test(
    name = "state_test",
    srcs = ["state_test.cc"],
    deps = [
        ":state",
        ":configurations",
        "//backend/index/memory:index",
        "//backend/store/memory:store",
        "//backend/depot/memory:depot",
        "//common:status_test_util",
        "//common:file_util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "configurations",
    deps = [
        ":state",
        "//backend/index/cache",
        "//backend/index/file:index",
        "//backend/index/memory:index",
        "//backend/store/file:store",
        "//backend/store/memory:store",
        "//backend/store/leveldb:store",
        "//backend/index/leveldb/multi_db:index",
        "//backend/depot/memory:depot",
        "//backend/depot/file:depot",
        "//backend/depot/leveldb:depot",
    ],
    hdrs = ["configurations.h"],
)

cc_library(
    name = "c_state",
    hdrs = ["c_state.h"],
    srcs = ["c_state.cc"],
    deps = [
        ":state",
        ":configurations",
        "@com_google_absl//absl/status:status",
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
  name = "libcarmen.so",
  linkshared = True,
  linkopts = select({
    "@platforms//os:osx": [
        # Needed to make resulting library moveable
        "-install_name @rpath/libcarmen.so",
    ],
    "//conditions:default": [],
  }),
  deps = [
     ":c_state",
  ],
)
