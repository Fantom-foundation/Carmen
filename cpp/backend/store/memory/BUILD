cc_library(
    name = "store",
    hdrs = ["store.h"],
   deps = [
    "@com_google_absl//absl/container:flat_hash_map",
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