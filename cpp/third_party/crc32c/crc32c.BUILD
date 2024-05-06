# Copyright (c) 2024 Fantom Foundation
# 
# Use of this software is governed by the Business Source License included
# in the LICENSE file and at fantom.foundation/bsl11.
# 
# Change Date: 2028-4-16
# 
# On the date above, in accordance with the Business Source License, use of
# this software will be governed by the GNU Lesser General Public License v3.

licenses(["notice"])

exports_files(["LICENSE"])

package(
    default_visibility = ["//visibility:public"],
)

genrule(
    name = "crc32c_config_h",
    srcs = ["@//third_party/crc32c:crc32c_config.h"],
    outs = ["include/crc32c/crc32c_config.h"],
    cmd = "cp $< $@",
)

cc_library(
    name = "crc32c",
    srcs = [
        "src/crc32c.cc",
        "src/crc32c_portable.cc",
    ],
    hdrs = [
        "include/crc32c/crc32c.h",
        "include/crc32c/crc32c_config.h",
        "src/crc32c_arm64.h",
        "src/crc32c_arm64_check.h",
        "src/crc32c_internal.h",
        "src/crc32c_prefetch.h",
        "src/crc32c_read_le.h",
        "src/crc32c_round_up.h",
        "src/crc32c_sse42.h",
        "src/crc32c_sse42_check.h",
    ],
    includes = ["include"],
)
