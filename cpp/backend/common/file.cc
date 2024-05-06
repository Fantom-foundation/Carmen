// Copyright (c) 2024 Fantom Foundation
// 
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
// 
// Change Date: 2028-4-16
// 
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

#include "backend/common/file.h"

#include <fcntl.h>
#include <unistd.h>

#include <cassert>
#include <cstdio>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "backend/common/page.h"
#include "common/fstream.h"
#include "common/status_util.h"

namespace carmen::backend {

// Creates the provided directory file path recursively. If the directory
// fails to be created, returns an error status.
absl::Status CreateDirectory(const std::filesystem::path& dir) {
  if (std::filesystem::exists(dir)) return absl::OkStatus();
  if (!dir.has_relative_path()) {
    return absl::InternalError(
        absl::StrFormat("Failed to create directory %s.", dir));
  }
  RETURN_IF_ERROR(CreateDirectory(dir.parent_path()));
  if (!std::filesystem::create_directory(dir)) {
    return absl::InternalError(
        absl::StrFormat("Failed to create directory %s.", dir));
  }
  return absl::OkStatus();
}

// Creates empty file at the provided file path. If the directory path
// does not exist, it is created. Returns ok status if the file was created
// successfully, otherwise returns the error status.
absl::Status CreateFile(const std::filesystem::path& path) {
  if (std::filesystem::exists(path)) {
    return absl::OkStatus();
  }
  // Create the directory path if it does not exist.
  RETURN_IF_ERROR(CreateDirectory(path.parent_path()));
  // Opening the file write-only first creates the file in case it does not
  // exist.
  ASSIGN_OR_RETURN(auto fs,
                   FStream::Open(path, std::ios::binary | std::ios::out));
  RETURN_IF_ERROR(fs.Close());
  return absl::OkStatus();
}

namespace internal {

namespace {

// Retain a 256 KiB aligned buffer of zeros for initializing disk space.
alignas(kFileSystemPageSize) static const std::array<char, 1 << 18> kZeros{};

}  // namespace

absl::StatusOr<FStreamFile> FStreamFile::Open(
    const std::filesystem::path& path) {
  RETURN_IF_ERROR(CreateFile(path));
  ASSIGN_OR_RETURN(
      auto fs,
      FStream::Open(path, std::ios::binary | std::ios::in | std::ios::out));
  RETURN_IF_ERROR(fs.Seekg(0, std::ios::end));
  ASSIGN_OR_RETURN(auto file_size, fs.Tellg());
  return FStreamFile(std::move(fs), file_size);
}

FStreamFile::FStreamFile(FStream fs, std::size_t file_size)
    : file_size_(file_size), data_(std::move(fs)) {}

FStreamFile::~FStreamFile() { Close().IgnoreError(); }

std::size_t FStreamFile::GetFileSize() const { return file_size_; }

absl::Status FStreamFile::Read(std::size_t pos, std::span<std::byte> span) {
  if (pos + span.size() > file_size_) {
    assert(pos >= file_size_ && "Reading non-aligned pages!");
    std::memset(span.data(), 0, span.size());
    return absl::OkStatus();
  }
  RETURN_IF_ERROR(data_.Seekg(pos));
  return data_.Read(span);
}

absl::Status FStreamFile::Write(std::size_t pos,
                                std::span<const std::byte> span) {
  // Grow file as needed.
  RETURN_IF_ERROR(GrowFileIfNeeded(pos + span.size()));
  RETURN_IF_ERROR(data_.Seekp(pos));
  return data_.Write(span);
}

absl::Status FStreamFile::Flush() { return data_.Flush(); }

absl::Status FStreamFile::Close() {
  if (data_.IsOpen()) {
    RETURN_IF_ERROR(Flush());
    return data_.Close();
  }
  return absl::OkStatus();
}

absl::Status FStreamFile::GrowFileIfNeeded(std::size_t needed) {
  if (file_size_ >= needed) {
    return absl::OkStatus();
  }
  RETURN_IF_ERROR(data_.Seekp(0, std::ios::end));
  while (file_size_ < needed) {
    auto step = std::min(kZeros.size(), needed - file_size_);
    RETURN_IF_ERROR(data_.Write(std::span(kZeros.data(), step)));
    file_size_ += step;
  }
  return absl::OkStatus();
}

absl::StatusOr<CFile> CFile::Open(const std::filesystem::path& path) {
  // Create the file if it does not exist.
  RETURN_IF_ERROR(CreateFile(path));
  // Open the file
  auto file = std::fopen(path.string().c_str(), "r+b");
  if (file == nullptr) {
    return absl::InternalError(
        absl::StrFormat("Failed to open file %s.", path));
  }
  // Seek to the end to get the file.
  if (std::fseek(file, 0, SEEK_END) != 0) {
    return absl::InternalError(
        absl::StrFormat("Failed to seek to end of file %s.", path));
  }
  // Get the file size.
  auto file_size = std::ftell(file);
  if (file_size == -1) {
    return GetStatusWithSystemError(
        absl::StatusCode::kInternal, errno,
        absl::StrFormat("Failed to get size of file %s.", path.string()));
  }
  return CFile(file, file_size);
}

CFile::CFile(std::FILE* file, std::size_t file_size)
    : file_size_(file_size), file_(file) {}

CFile::CFile(CFile&& file) noexcept
    : file_size_(file.file_size_), file_(file.file_) {
  file.file_ = nullptr;
}

CFile::~CFile() { Close().IgnoreError(); }

std::size_t CFile::GetFileSize() const { return file_size_; }

absl::Status CFile::Read(std::size_t pos, std::span<std::byte> span) {
  if (file_ == nullptr) {
    return absl::InternalError("File is not open.");
  }
  if (pos + span.size() > file_size_) {
    assert(pos >= file_size_ && "Reading non-aligned pages!");
    std::memset(span.data(), 0, span.size());
    return absl::OkStatus();
  }
  if (std::fseek(file_, pos, SEEK_SET) != 0) {
    return absl::InternalError(
        absl::StrFormat("Failed to seek to position %d.", pos));
  }
  auto len = std::fread(span.data(), sizeof(std::byte), span.size(), file_);
  if (len != span.size()) {
    if (std::feof(file_)) {
      return absl::InternalError(absl::StrFormat(
          "Failed to read %d bytes from file. End of file reached.",
          span.size()));
    }
    if (std::ferror(file_)) {
      return absl::InternalError(
          absl::StrFormat("Failed to read %d bytes from file.", span.size()));
    }
    return absl::InternalError(
        absl::StrFormat("Read different number of bytes than requested."
                        " Requested: %d, Read: %d.",
                        span.size(), len));
  }
  return absl::OkStatus();
}

absl::Status CFile::Write(std::size_t pos, std::span<const std::byte> span) {
  if (file_ == nullptr) {
    return absl::InternalError("File is not open.");
  }
  // Grow file as needed.
  RETURN_IF_ERROR(GrowFileIfNeeded(pos + span.size()));
  if (std::fseek(file_, pos, SEEK_SET) != 0) {
    return absl::InternalError(
        absl::StrFormat("Failed to seek to position %d.", pos));
  }
  auto len = std::fwrite(span.data(), sizeof(std::byte), span.size(), file_);
  if (len != span.size()) {
    if (std::ferror(file_)) {
      return absl::InternalError(
          absl::StrFormat("Failed to write %d bytes to file.", span.size()));
    }
    return absl::InternalError(
        absl::StrFormat("Wrote different number of bytes than requested."
                        " Requested: %d, Written: %d.",
                        span.size(), len));
  }
  return absl::OkStatus();
}

absl::Status CFile::Flush() {
  if (file_ != nullptr && std::fflush(file_) == EOF) {
    return absl::InternalError("Failed to flush file.");
  }
  return absl::OkStatus();
}

absl::Status CFile::Close() {
  if (file_ != nullptr) {
    RETURN_IF_ERROR(Flush());
    if (std::fclose(file_) == EOF) {
      return absl::InternalError("Failed to close file.");
    }
    file_ = nullptr;
  }
  return absl::OkStatus();
}

absl::Status CFile::GrowFileIfNeeded(std::size_t needed) {
  if (file_size_ >= needed) {
    return absl::OkStatus();
  }
  if (std::fseek(file_, 0, SEEK_END) != 0) {
    return absl::InternalError(
        absl::StrFormat("Failed to seek to end of file."));
  }
  while (file_size_ < needed) {
    auto step = std::min(kZeros.size(), needed - file_size_);
    auto len = std::fwrite(kZeros.data(), sizeof(std::byte), step, file_);
    if (len != step) {
      if (std::ferror(file_)) {
        return absl::InternalError(
            absl::StrFormat("Failed to write %d bytes to file.", step));
      }
      return absl::InternalError(
          absl::StrFormat("Wrote different number of bytes than requested."
                          " Requested: %d, Written: %d.",
                          step, len));
    }
    file_size_ += step;
  }
  return absl::OkStatus();
}

absl::StatusOr<PosixFile> PosixFile::Open(const std::filesystem::path& path) {
  // Create the parent directory.
  RETURN_IF_ERROR(CreateDirectory(path.parent_path()));
  int fd;
#ifdef O_DIRECT
  // When using O_DIRECT, all read/writes must use aligned memory locations!
  fd = open(path.string().c_str(), O_CREAT | O_DIRECT | O_RDWR);
#else
  fd = open(path.string().c_str(), O_CREAT | O_RDWR);
#endif
  // Open the file.
  if (fd == -1) {
    return GetStatusWithSystemError(
        absl::StatusCode::kInternal, errno,
        absl::StrFormat("Failed to open file %s.", path.string()));
  }
  // Seek to the end to get the file.
  off_t size = lseek(fd, 0, SEEK_END);
  if (size == -1) {
    return GetStatusWithSystemError(
        absl::StatusCode::kInternal, errno,
        absl::StrFormat("Failed to seek to end of file %s.", path.string()));
  }
  return PosixFile(fd, size);
}

PosixFile::PosixFile(int fd, std::size_t file_size)
    : file_size_(file_size), fd_(fd) {}

PosixFile::PosixFile(PosixFile&& file) noexcept
    : file_size_(file.file_size_), fd_(file.fd_) {
  file.fd_ = -1;
}

PosixFile::~PosixFile() { Close().IgnoreError(); }

std::size_t PosixFile::GetFileSize() const { return file_size_; }

absl::Status PosixFile::Read(std::size_t pos, std::span<std::byte> span) {
  if (fd_ < 0) {
    return absl::InternalError("File is not open.");
  }
  if (pos + span.size() > file_size_) {
    assert(pos >= file_size_ && "Reading non-aligned pages!");
    std::memset(span.data(), 0, span.size());
    return absl::OkStatus();
  }
  RETURN_IF_ERROR(GrowFileIfNeeded(pos + span.size()));
  if (lseek(fd_, pos, SEEK_SET) == -1) {
    return GetStatusWithSystemError(
        absl::StatusCode::kInternal, errno,
        absl::StrFormat("Failed to seek to position %d.", pos));
  }
  auto len = read(fd_, span.data(), span.size());
  if (len != static_cast<ssize_t>(span.size())) {
    if (len == -1) {
      return GetStatusWithSystemError(
          absl::StatusCode::kInternal, errno,
          absl::StrFormat("Failed to read %d bytes from file.", span.size()));
    }
    return absl::InternalError(
        absl::StrFormat("Failed to read %d bytes from file.", span.size()));
  }
  return absl::OkStatus();
}

absl::Status PosixFile::Write(std::size_t pos,
                              std::span<const std::byte> span) {
  if (fd_ < 0) {
    return absl::InternalError("File is not open.");
  }
  // Grow file as needed.
  RETURN_IF_ERROR(GrowFileIfNeeded(pos + span.size()));
  if (lseek(fd_, pos, SEEK_SET) == -1) {
    return GetStatusWithSystemError(
        absl::StatusCode::kInternal, errno,
        absl::StrFormat("Failed to seek to position %d.", pos));
  }
  auto len = write(fd_, span.data(), span.size());
  if (len != static_cast<ssize_t>(span.size())) {
    return GetStatusWithSystemError(
        absl::StatusCode::kInternal, errno,
        absl::StrFormat("Wrote different number of bytes than requested."
                        "Wrote %d, requested %d.",
                        len, span.size()));
  }
  return absl::OkStatus();
}

absl::Status PosixFile::Flush() {
  if (fd_ >= 0 && fsync(fd_) == -1) {
    return GetStatusWithSystemError(absl::StatusCode::kInternal, errno,
                                    "Failed to flush file.");
  }
  return absl::OkStatus();
}

absl::Status PosixFile::Close() {
  if (fd_ >= 0) {
    RETURN_IF_ERROR(Flush());
    if (close(fd_) == -1) {
      return GetStatusWithSystemError(absl::StatusCode::kInternal, errno,
                                      "Failed to close file.");
    }
    fd_ = -1;
  }
  return absl::OkStatus();
}

absl::Status PosixFile::GrowFileIfNeeded(std::size_t needed) {
  if (file_size_ >= needed) {
    return absl::OkStatus();
  }
  auto offset = lseek(fd_, 0, SEEK_END);
  if (offset != static_cast<off_t>(file_size_)) {
    return GetStatusWithSystemError(
        absl::StatusCode::kInternal, errno,
        absl::StrFormat(
            "Failed to seek to end of file. Expected offset %d, got %d.",
            file_size_, offset));
  }
  while (file_size_ < needed) {
    auto step = std::min(kZeros.size(), needed - file_size_);
    auto len = write(fd_, kZeros.data(), step);
    if (len != static_cast<ssize_t>(step)) {
      return GetStatusWithSystemError(
          absl::StatusCode::kInternal, errno,
          absl::StrFormat("Wrote different number of bytes than requested."
                          "Expected: %d, actual: %d",
                          step, len));
    }
    file_size_ += step;
  }
  return absl::OkStatus();
}

}  // namespace internal
}  // namespace carmen::backend
