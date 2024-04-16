/*
 * Copyright (c) 2024 Fantom Foundation
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE file and at fantom.foundation/bsl11.
 *
 * Change Date: 2028-4-16
 *
 * On the date above, in accordance with the Business Source License, use of
 * this software will be governed by the GNU Lesser General Public Licence v3.
 */

#include "fstream.h"

#include <fstream>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"

namespace carmen {

absl::StatusOr<FStream> FStream::Open(const std::filesystem::path& path,
                                      std::ios::openmode mode) {
  std::fstream fs;
  fs.open(path, mode);
  if (fs.is_open()) return FStream(std::move(fs), path);
  return absl::InternalError(
      absl::StrFormat("Failed to open file %s.", path.string()));
}

absl::Status FStream::Seekg(std::size_t offset, std::ios::seekdir dir) {
  fs_.seekg(offset, dir);
  if (fs_.good()) return absl::OkStatus();
  return absl::InternalError(absl::StrFormat(
      "Failed to seek to position %d in file %s.", offset, path_.string()));
}

absl::StatusOr<std::size_t> FStream::Tellg() {
  auto pos = fs_.tellg();
  if (fs_.good() && pos != -1) return pos;
  return absl::InternalError(
      absl::StrFormat("Failed to get position in file %s.", path_.string()));
}

absl::Status FStream::Seekp(std::size_t offset, std::ios::seekdir dir) {
  fs_.seekg(offset, dir);
  if (fs_.good()) return absl::OkStatus();
  return absl::InternalError(absl::StrFormat(
      "Failed to seek to position %d in file %s.", offset, path_.string()));
}

absl::StatusOr<std::size_t> FStream::Tellp() {
  auto pos = fs_.tellp();
  if (fs_.good() && pos != -1) return pos;
  return absl::InternalError(
      absl::StrFormat("Failed to get position in file %s.", path_.string()));
}

absl::Status FStream::Flush() {
  fs_.flush();
  if (fs_.good()) return absl::OkStatus();
  return absl::InternalError(
      absl::StrFormat("Failed to flush file %s.", path_.string()));
}

absl::Status FStream::Close() {
  fs_.close();
  if (fs_.good()) return absl::OkStatus();
  return absl::InternalError(
      absl::StrFormat("Failed to close file %s.", path_.string()));
}

bool FStream::IsOpen() const { return fs_.is_open(); }

}  // namespace carmen
