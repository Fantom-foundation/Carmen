/*
 * Copyright (c) 2024 Fantom Foundation
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at fantom.foundation/bsl11.
 *
 * Change Date: 2028-4-16
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by the GNU Lesser General Public Licence v3.
 */

#include "common/file_util.h"

#include <cstdio>
#include <filesystem>
#include <fstream>
#include <random>
#include <sstream>

namespace carmen {

namespace {

std::filesystem::path GetTempFilePath(std::string_view prefix, int i) {
  std::stringstream out;
  out << prefix << "_" << i << ".dat";
  return std::filesystem::temp_directory_path() / out.str();
}

std::filesystem::path GetTempDirPath(std::string_view prefix, int i) {
  std::stringstream out;
  out << prefix << "_" << i;
  return std::filesystem::temp_directory_path() / out.str();
}

}  // namespace

TempFile::TempFile(std::string_view prefix) {
  // Look for a file name that is not yet used.
  std::random_device rd;
  std::default_random_engine rnd(rd());
  std::uniform_int_distribution<int> dist;
  path_ = GetTempFilePath(prefix, dist(rnd));
  while (std::filesystem::exists(path_)) {
    path_ = GetTempFilePath(prefix, dist(rnd));
  }
  // Create that file to technically allocate that name.
  // Note: it may happen that a concurrent process is taking ownership of the
  // same file at the same time, but since this is a test utility, we accept
  // that risk.
  std::fstream(path_, std::ios::out);
}

TempFile::~TempFile() {
  // Delete the owned temporary file.
  if (!path_.empty()) {
    std::filesystem::remove(path_);
  }
}

const std::filesystem::path& TempFile::GetPath() const { return path_; }

TempDir::TempDir(std::string_view prefix) {
  // Look for a file name that is not yet used.
  std::random_device rd;
  std::default_random_engine rnd(rd());
  std::uniform_int_distribution<int> dist;
  path_ = GetTempDirPath(prefix, dist(rnd));
  while (std::filesystem::exists(path_)) {
    path_ = GetTempDirPath(prefix, dist(rnd));
  }
  // Create that directory to technically allocate that name.
  // Note: it may happen that a concurrent process is taking ownership of the
  // same directory at the same time, but since this is a test utility, we
  // accept that risk.
  std::filesystem::create_directories(path_);
}

TempDir::~TempDir() {
  // Delete the owned temporary file.
  if (!path_.empty()) {
    std::filesystem::remove_all(path_);
  }
}

const std::filesystem::path& TempDir::GetPath() const { return path_; }

}  // namespace carmen
