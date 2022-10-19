cc_library(
    name = "state",
    hdrs = ["state.h"],
    deps = [
        "//common:account_state",
        "//common:type",
    ]
)

cc_test(
    name = "state_test",
    srcs = ["state_test.cc"],
    deps = [
        ":state",
        "//backend/index/memory:index",
        "//backend/store/memory:store",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "c_state",
    hdrs = ["c_state.h"],
    srcs = ["c_state.cc"],
    deps = [
        ":state",
        "//backend/index/memory:index",
        "//backend/store/file:store",
        "//backend/store/memory:store",
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
  deps = [
     ":c_state",
  ],
)