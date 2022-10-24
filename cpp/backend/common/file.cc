#include "backend/common/file.h"

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

RawFile::RawFile(std::filesystem::path file) {
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

RawFile::~RawFile() { data_.close(); }

std::size_t RawFile::GetFileSize() { return file_size_; }

void RawFile::Read(std::size_t pos, std::span<std::byte> span) {
  GrowFileIfNeeded(pos + span.size());
  data_.seekg(pos);
  data_.read(reinterpret_cast<char*>(span.data()), span.size());
}

void RawFile::Write(std::size_t pos, std::span<const std::byte> span) {
  // Grow file as needed.
  GrowFileIfNeeded(pos + span.size());
  data_.seekp(pos);
  data_.write(reinterpret_cast<const char*>(span.data()), span.size());
}

void RawFile::Flush() { std::flush(data_); }

void RawFile::Close() {
  Flush();
  data_.close();
}

void RawFile::GrowFileIfNeeded(std::size_t needed) {
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

}  // namespace internal
}  // namespace carmen::backend
