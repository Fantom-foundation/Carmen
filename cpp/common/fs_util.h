#pragma once

#include <cerrno>
#include <fstream>

#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "common/file_util.h"
#include "common/status_util.h"

namespace carmen {

absl::Status fs_open(std::fstream& fs, const std::string& path,
                     std::ios_base::openmode mode) {
  fs.open(path, mode);
  if (!fs.is_open()) {
    return GetStatusWithSystemError(absl::StatusCode::kInternal,
                                    "Failed to open file.");
  }
  return absl::OkStatus();
}
}  // namespace carmen
