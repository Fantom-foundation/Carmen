# Copyright (c) 2024 Fantom Foundation
#
# Use of this software is governed by the Business Source License included
# in the LICENSE file and at fantom.foundation/bsl11.
#
# Change Date: 2028-4-16
#
# On the date above, in accordance with the Business Source License, use of
# this software will be governed by the GNU Lesser General Public License v3.

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
