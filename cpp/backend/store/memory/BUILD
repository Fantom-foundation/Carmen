cc_library(
    name = "store",
    hdrs = ["store.h"],
    deps = [
      "//common:type",
      "//common:hash",
    ],
)

cc_test(
    name = "store_test",
    srcs = ["store_test.cc"],
    deps = [
        ":store", 
        "@com_google_googletest//:gtest_main",
    ],
)