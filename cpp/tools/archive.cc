// This file provides an executable that can be used to perform operations on
// archive files.

#include "state/archive.h"

#include <stdlib.h>

#include <iostream>
#include <string_view>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/time/time.h"
#include "common/status_util.h"

// To run this binary with bazel, use the following command:
//   bazel run -c opt //tools:archive <args>

namespace carmen {
namespace {

absl::Status PrintStats(int argc, char** argv) {
  if (argc != 3) {
    std::cout << "Stats needs exactly one argument: <archive_file>\n";
    return absl::InvalidArgumentError("missing arguments");
  }
  std::string_view path = argv[2];
  std::cout << "Opening " << path << " ..\n";
  ASSIGN_OR_RETURN(auto archive, Archive::Open(path));
  ASSIGN_OR_RETURN(auto height, archive.GetLatestBlock());
  std::cout << "\tBlock height: " << height << "\n";
  ASSIGN_OR_RETURN(auto hash, archive.GetHash(height));
  std::cout << "\tArchive Hash: " << hash << "\n";
  return absl::OkStatus();
}

absl::Status Verify(int argc, char** argv) {
  if (argc != 3) {
    std::cout << "Verify needs exactly one argument: <archive_file>\n";
    return absl::InvalidArgumentError("missing arguments");
  }
  std::string_view path = argv[2];
  std::cout << "Opening " << path << " ..\n";
  ASSIGN_OR_RETURN(auto archive, Archive::Open(path));
  ASSIGN_OR_RETURN(auto height, archive.GetLatestBlock());
  std::cout << "\tBlock height: " << height << "\n";
  ASSIGN_OR_RETURN(auto hash, archive.GetHash(height));
  std::cout << "\tArchive Hash: " << hash << "\n";
  std::cout << "\tRunning verification ...\n";
  auto start = absl::Now();
  // TODO: add some progress reporting.
  auto verify_result = archive.Verify(height, hash);
  auto duration = absl::Now() - start;
  if (verify_result.ok()) {
    std::cout << "\tVerification: successful (took ";
    auto sec = absl::ToInt64Seconds(duration);
    std::cout << absl::StrFormat("%d:%02d", sec / 60, sec % 60);
    std::cout << ")\n";
  } else {
    std::cout << "\tVerification: failed\n";
    std::cout << "\t\t" << verify_result.message() << "\n";
  }
  return absl::OkStatus();
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
