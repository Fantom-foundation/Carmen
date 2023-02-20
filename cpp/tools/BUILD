cc_binary(
    name = "archive",
    srcs = ["archive.cc"],
    deps = [
        "//archive",
        "//archive/leveldb:archive",
        "//archive/sqlite:archive",
        "//third_party/gperftools:profiler",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/time",
    ],
)
