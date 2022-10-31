#include "backend/common/file.h"

#include <fcntl.h>
#include <unistd.h>

#include <cassert>

namespace carmen::backend {

namespace internal {

namespace {

// Creates the provided directory file path recursively. Returns true on
// success, false otherwise.
bool CreateDirectory(std::filesystem::path dir) {
  if (std::filesystem::exists(dir)) return true;
  if (!dir.has_relative_path()) return false;
  return CreateDirectory(dir.parent_path()) &&
         std::filesystem::create_directory(dir);
}

}  // namespace

FStreamFile::FStreamFile(std::filesystem::path file) {
  // Create the parent directory.
  CreateDirectory(file.parent_path());
  // Opening the file write-only first creates the file in case it does not
  // exist.
  data_.open(file, std::ios::binary | std::ios::out);
  data_.close();
  // However, we need the file open in read & write mode.
  data_.open(file, std::ios::binary | std::ios::out | std::ios::in);
  data_.seekg(0, std::ios::end);
  file_size_ = data_.tellg();
}

FStreamFile::~FStreamFile() { Close(); }

std::size_t FStreamFile::GetFileSize() { return file_size_; }

void FStreamFile::Read(std::size_t pos, std::span<std::byte> span) {
  if (pos + span.size() > file_size_) {
    assert(pos >= file_size_ && "Reading non-aligned pages!");
    memset(span.data(), 0, span.size());
    return;
  }
  data_.seekg(pos);
  data_.read(reinterpret_cast<char*>(span.data()), span.size());
}

void FStreamFile::Write(std::size_t pos, std::span<const std::byte> span) {
  // Grow file as needed.
  GrowFileIfNeeded(pos + span.size());
  data_.seekp(pos);
  data_.write(reinterpret_cast<const char*>(span.data()), span.size());
}

void FStreamFile::Flush() { std::flush(data_); }

void FStreamFile::Close() {
  Flush();
  data_.close();
}

void FStreamFile::GrowFileIfNeeded(std::size_t needed) {
  // Retain a 256 KiB buffer of zeros for initializing disk space.
  constexpr static std::size_t kStepSize = 1 << 18;
  static auto kZeros = std::make_unique<const std::array<char, kStepSize>>();
  if (file_size_ >= needed) {
    return;
  }
  data_.seekp(0, std::ios::end);
  while (file_size_ < needed) {
    auto step = std::min(kStepSize, needed - file_size_);
    data_.write(kZeros->data(), step);
    file_size_ += step;
  }
}

CFile::CFile(std::filesystem::path file) {
  // Create the parent directory.
  CreateDirectory(file.parent_path());
  // Append mode will create the file if does not exist.
  file_ = std::fopen(file.string().c_str(), "a");
  std::fclose(file_);
  // But for read/write we need the file to be openend in expended read mode.
  file_ = std::fopen(file.string().c_str(), "r+b");
  assert(file_);
  [[maybe_unused]] auto succ = std::fseek(file_, 0, SEEK_END);
  assert(succ == 0);
  file_size_ = std::ftell(file_);
}

CFile::~CFile() { Close(); }

std::size_t CFile::GetFileSize() { return file_size_; }

void CFile::Read(std::size_t pos, std::span<std::byte> span) {
  if (file_ == nullptr) return;
  if (pos + span.size() > file_size_) {
    assert(pos >= file_size_ && "Reading non-aligned pages!");
    memset(span.data(), 0, span.size());
    return;
  }
  [[maybe_unused]] auto succ = std::fseek(file_, pos, SEEK_SET);
  assert(succ == 0);
  [[maybe_unused]] auto len =
      std::fread(span.data(), sizeof(std::byte), span.size(), file_);
  assert(len == span.size());
}

void CFile::Write(std::size_t pos, std::span<const std::byte> span) {
  if (file_ == nullptr) return;
  // Grow file as needed.
  GrowFileIfNeeded(pos + span.size());
  [[maybe_unused]] auto succ = std::fseek(file_, pos, SEEK_SET);
  assert(succ == 0);
  [[maybe_unused]] auto len =
      std::fwrite(span.data(), sizeof(std::byte), span.size(), file_);
  assert(len == span.size());
}

void CFile::Flush() {
  if (file_ == nullptr) return;
  std::fflush(file_);
}

void CFile::Close() {
  if (file_ == nullptr) return;
  Flush();
  fclose(file_);
  file_ = nullptr;
}

void CFile::GrowFileIfNeeded(std::size_t needed) {
  // Retain a 256 KiB buffer of zeros for initializing disk space.
  constexpr static std::size_t kStepSize = 1 << 18;
  static auto kZeros = std::make_unique<const std::array<char, kStepSize>>();
  if (file_size_ >= needed) {
    return;
  }
  std::fseek(file_, 0, SEEK_END);
  while (file_size_ < needed) {
    auto step = std::min(kStepSize, needed - file_size_);
    [[maybe_unused]] auto len =
        fwrite(kZeros->data(), sizeof(std::byte), step, file_);
    assert(len == step);
    file_size_ += step;
  }
}

PosixFile::PosixFile(std::filesystem::path file) {
  // Create the parent directory.
  CreateDirectory(file.parent_path());
  //  When this is enabled, all read/writes must use aligned memory locations!
  fd_ = open(file.string().c_str(), O_CREAT | O_DIRECT | O_RDWR);
  assert(fd_ >= 0);
  off_t size = lseek(fd_, 0, SEEK_END);
  if (size == -1) {
    perror("Error getting file size: ");
  }
  file_size_ = size;
}

PosixFile::~PosixFile() { Close(); }

std::size_t PosixFile::GetFileSize() { return file_size_; }

void PosixFile::Read(std::size_t pos, std::span<std::byte> span) {
  if (fd_ < 0) return;
  if (pos + span.size() > file_size_) {
    assert(pos >= file_size_ && "Reading non-aligned pages!");
    memset(span.data(), 0, span.size());
    return;
  }
  GrowFileIfNeeded(pos + span.size());
  lseek(fd_, pos, SEEK_SET);
  [[maybe_unused]] auto len = read(fd_, span.data(), span.size());
  assert(len == static_cast<ssize_t>(span.size()));
}

void PosixFile::Write(std::size_t pos, std::span<const std::byte> span) {
  if (fd_ < 0) return;
  // Grow file as needed.
  GrowFileIfNeeded(pos + span.size());
  lseek(fd_, pos, SEEK_SET);
  [[maybe_unused]] auto len = write(fd_, span.data(), span.size());
  assert(len == static_cast<ssize_t>(span.size()));
}

void PosixFile::Flush() {
  if (fd_ < 0) return;
  fsync(fd_);
}

void PosixFile::Close() {
  if (fd_ < 0) return;
  Flush();
  close(fd_);
  fd_ = -1;
}

void PosixFile::GrowFileIfNeeded(std::size_t needed) {
  // Retain a 256 KiB buffer of zeros for initializing disk space.
  constexpr static std::size_t kStepSize = 1 << 18;
  static auto kZeros = std::make_unique<ArrayPage<int, kStepSize>>();
  if (file_size_ >= needed) {
    return;
  }
  [[maybe_unused]] auto offset = lseek(fd_, 0, SEEK_END);
  assert(offset == static_cast<off_t>(file_size_));
  while (file_size_ < needed) {
    auto step = std::min(kStepSize, needed - file_size_);
    auto len = write(fd_, kZeros->AsRawData().data(), step);
    if (len < 0) {
      perror("Error growing file");
    }
    assert(len == static_cast<ssize_t>(step));
    file_size_ += step;
  }
}

}  // namespace internal
}  // namespace carmen::backend
