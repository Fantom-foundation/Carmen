/*
 * Copyright (c) 2024 Fantom Foundation
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE file and at fantom.foundation/bsl11.
 *
 * Change Date: 2028-4-16
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by the GNU Lesser General Public Licence v3.
 */

// This file provides an executable that can be used to perform operations on
// archive files.

#include "archive/archive.h"

#include <stdlib.h>

#include <iostream>
#include <string_view>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/time/time.h"
#include "archive/leveldb/archive.h"
#include "archive/sqlite/archive.h"
#include "common/status_util.h"

// To run this binary with bazel, use the following command:
//   bazel run -c opt //tools:archive <args>

namespace carmen {
namespace {

using ::carmen::archive::leveldb::LevelDbArchive;
using ::carmen::archive::sqlite::SqliteArchive;

template <Archive Archive>
absl::Status PrintStats(std::string_view path) {
  std::cout << "Opening " << path << " ..\n";
  ASSIGN_OR_RETURN(auto archive, Archive::Open(path));
  ASSIGN_OR_RETURN(auto height, archive.GetLatestBlock());
  std::cout << "\tBlock height: " << height << "\n";
  ASSIGN_OR_RETURN(auto hash, archive.GetHash(height));
  std::cout << "\tArchive Hash: " << hash << "\n";
  return archive.Close();
}

absl::Status PrintStats(int argc, char** argv) {
  if (argc != 3) {
    std::cout << "Stats needs exactly one argument: <archive_file>\n";
    return absl::InvalidArgumentError("missing arguments");
  }
  std::string_view path = argv[2];
  if (path.ends_with("sqlite")) {
    return PrintStats<SqliteArchive>(path);
  }
  return PrintStats<LevelDbArchive>(path);
}

template <Archive Archive>
absl::Status Verify(std::string_view path) {
  std::cout << "Opening " << path << " ..\n";
  ASSIGN_OR_RETURN(auto archive, Archive::Open(path));
  ASSIGN_OR_RETURN(auto height, archive.GetLatestBlock());
  std::cout << "\tBlock height: " << height << "\n";
  auto start = absl::Now();
  ASSIGN_OR_RETURN(auto hash, archive.GetHash(height));
  auto duration = absl::Now() - start;
  std::cout << "\tArchive Hash: " << hash << " (took ";
  auto sec = absl::ToInt64Seconds(duration);
  std::cout << absl::StrFormat("%d:%02d", sec / 60, sec % 60) << ")\n";
  std::cout << "\tRunning verification ...\n";
  start = absl::Now();
  auto verify_result =
      archive.Verify(height, hash, [&](std::string_view phase) {
        auto time = absl::Now() - start;
        auto sec = absl::ToInt64Seconds(time);
        std::cout << "\t\tt=" << absl::StrFormat("%3d:%02d", sec / 60, sec % 60)
                  << ": " << phase << " ... \n";
      });
  duration = absl::Now() - start;
  if (verify_result.ok()) {
    std::cout << "\tVerification: successful (took ";
    sec = absl::ToInt64Seconds(duration);
    std::cout << absl::StrFormat("%d:%02d", sec / 60, sec % 60);
    std::cout << ")\n";
  } else {
    std::cout << "\tVerification: failed\n";
    std::cout << "\t\t" << verify_result.message() << "\n";
  }
  return archive.Close();
}

absl::Status Verify(int argc, char** argv) {
  if (argc != 3) {
    std::cout << "Verify needs exactly one argument: <archive_file>\n";
    return absl::InvalidArgumentError("missing arguments");
  }
  std::string_view path = argv[2];
  if (path.ends_with("sqlite")) {
    return PrintStats<SqliteArchive>(path);
  }
  return Verify<LevelDbArchive>(path);
}

absl::Status Main(int argc, char** argv) {
  if (argc < 2) {
    std::cout << "Missing command argument:\n";
    std::cout << "\tstats  ... prints some summary information of an archive\n";
    std::cout << "\tverify ... verifies the integrity of an archive\n";
    return absl::InvalidArgumentError("missing command argument");
  }

  std::string_view cmd = argv[1];
  if (cmd == "stats") {
    return PrintStats(argc, argv);
  } else if (cmd == "verify") {
    return Verify(argc, argv);
  }
  return absl::InvalidArgumentError(
      absl::StrFormat("Unknown command: %s", cmd));
}

}  // namespace
}  // namespace carmen

int main(int argc, char** argv) {
  auto status = carmen::Main(argc, argv);
  if (status.ok()) {
    return EXIT_SUCCESS;
  }
  std::cerr << "Execution failed: " << status.message() << "\n";
  return EXIT_FAILURE;
}
