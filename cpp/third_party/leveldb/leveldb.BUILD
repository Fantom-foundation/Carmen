licenses(["notice"])

exports_files(["LICENSE"])

package(
    default_visibility = ["//visibility:public"],
)

genrule(
    name = "port_config_h",
    srcs = ["@//third_party/leveldb:port_config.h"],
    outs = ["port/port_config.h"],
    cmd = "cp $< $@",
)

genrule(
    name = "port_h",
    srcs = ["@//third_party/leveldb:port.h"],
    outs = ["port/port.h"],
    cmd = "cp $< $@",
)

cc_library(
    name = "leveldb",
    srcs = glob(
        ["**/*.cc"],
        exclude = [
            "doc/**",
            "**/*_test.cc",
            "util/testutil.cc",
            "benchmarks/**",
            "util/*windows*",
            "db/leveldbutil.cc",
        ],
    ),
    hdrs = glob(
        ["**/*.h"],
        exclude = [
            "doc/**",
            "util/*windows*",
            "util/testutil.h",
            "port/port.h",
        ],
    ) + [
        ":port_h",
        ":port_config_h",
    ],
    copts = ["-Wno-unused-parameter"],
    defines = [
        "LEVELDB_PLATFORM_POSIX=1",
        "LEVELDB_IS_BIG_ENDIAN=0",
        "NDEBUG",
    ],
    includes = [
        ".",
        "include",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "@com_github_google_crc32c//:crc32c",
        "@com_github_google_snappy//:snappy",
    ],
)
