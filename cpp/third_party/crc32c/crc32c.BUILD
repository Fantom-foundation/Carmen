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
        "src/crc32c_sse42.h",
        "src/crc32c_sse42_check.h",
        "src/crc32c_round_up.h",
    ],
    includes = ["include"],
)
