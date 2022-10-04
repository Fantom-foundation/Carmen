#include "backend/store/file/file.h"

namespace carmen::backend::store {

namespace internal {

RawFile::RawFile(std::filesystem::path file) {
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

void RawFile::GrowFileIfNeeded(std::size_t needed) {
  if (file_size_ >= needed) {
    return;
  }
  data_.seekp(0, std::ios::end);
  while (file_size_ < needed) {
    data_.put(0);
    file_size_++;
  }
}

}  // namespace internal
}  // namespace carmen::backend::store
