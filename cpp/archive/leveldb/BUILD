# Copyright (c) 2024 Fantom Foundation
# 
# Use of this software is governed by the Business Source License included
# in the LICENSE file and at fantom.foundation/bsl11.
# 
# Change Date: 2028-4-16
# 
# On the date above, in accordance with the Business Source License, use
# of this software will be governed by the GNU Lesser General Public Licence v3.

cc_library(
    name = "archive",
    srcs = ["archive.cc"],
    hdrs = ["archive.h"],
    visibility = [
        "//state:__subpackages__",
        "//tools:__subpackages__",
    ],
    deps = [
        ":keys",
        ":values",
        "//backend/common/leveldb",
        "//common:byte_util",
        "//common:status_util",
        "//common:type",
        "//state:update",
        "@com_google_absl//absl/container:btree",
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
        "//archive:archive_test_suite",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "encoding",
    srcs = ["encoding.cc"],
    hdrs = ["encoding.h"],
    deps = [
        "//common:type",
    ],
)

cc_test(
    name = "encoding_test",
    srcs = ["encoding_test.cc"],
    deps = [
        ":encoding",
        "//common:type",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "keys",
    srcs = ["keys.cc"],
    hdrs = ["keys.h"],
    deps = [
        ":encoding",
        "//common:status_util",
        "//common:type",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_test(
    name = "keys_test",
    srcs = ["keys_test.cc"],
    deps = [
        ":keys",
        "//common:status_test_util",
        "//common:type",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "values",
    srcs = ["values.cc"],
    hdrs = ["values.h"],
    deps = [
        ":encoding",
        ":keys",
        "//common:status_util",
        "//common:type",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_test(
    name = "values_test",
    srcs = ["values_test.cc"],
    deps = [
        ":values",
        "//common:status_test_util",
        "//common:type",
        "@com_google_googletest//:gtest_main",
    ],
)
