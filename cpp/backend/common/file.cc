#include "backend/common/file.h"

#include <fcntl.h>
#include <unistd.h>

#include <cassert>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "common/status_util.h"

namespace carmen::backend {

namespace internal {
// Creates the provided directory file path recursively. Returns true on
// success, false otherwise.
bool CreateDirectoryInternal(const std::filesystem::path& dir) {
  if (std::filesystem::exists(dir)) return true;
  if (!dir.has_relative_path()) return false;
  return CreateDirectoryInternal(dir.parent_path()) &&
         std::filesystem::create_directory(dir);
}
} // namespace internal

// Creates the provided directory file path recursively. If the directory
// fails to be created, returns an error status.
absl::Status CreateDirectory(const std::filesystem::path& dir) {
  if (!internal::CreateDirectoryInternal(dir)) {
    return GetStatusWithSystemError(
            absl::StatusCode::kInternal,
            absl::StrFormat("Failed to create directory %s.", dir.string()));
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
  std::fstream fs;
  fs.open(path, std::ios::binary | std::ios::out);
  if (!fs.is_open()) {
    return absl::InternalError(absl::StrFormat(
        "Failed to open file %s for writing", path.string()));
  }
  fs.close();
  if (!fs.good()) {
    return absl::InternalError(absl::StrFormat(
        "Failed to close file %s after writing", path.string()));
  }
  return absl::OkStatus();
}

namespace internal {

absl::StatusOr<FStreamFile> FStreamFile::Open(const std::filesystem::path& path) {
  RETURN_IF_ERROR(CreateFile(path));
  std::fstream fs;
  fs.open(path, std::ios::binary | std::ios::out | std::ios::in);
  if (!fs.is_open()) {
    return absl::InternalError(absl::StrFormat(
        "Failed to open file %s", path.string()));
  }
  fs.seekg(0, std::ios::end);
  if (!fs.good()) {
  return absl::InternalError(absl::StrFormat(
          "Failed to seek to end of file %s", path.string()));
  }
  auto file_size = fs.tellg();
  if (file_size == -1) {
    return absl::InternalError(absl::StrFormat(
        "Failed to get size of file %s", path.string()));
  }
  return FStreamFile(std::move(fs), file_size);
}

FStreamFile::FStreamFile(std::fstream fs, std::size_t file_size) : file_size_(file_size), data_(std::move(fs)) {}

FStreamFile::~FStreamFile() { Close().IgnoreError(); }

std::size_t FStreamFile::GetFileSize() const { return file_size_; }

absl::Status FStreamFile::Read(std::size_t pos, std::span<std::byte> span) {
  if (pos + span.size() > file_size_) {
    assert(pos >= file_size_ && "Reading non-aligned pages!");
    std::memset(span.data(), 0, span.size());
    return absl::OkStatus();
  }
  data_.seekg(pos);
  if (!data_.good()) {
    return absl::InternalError("Failed to seek to position. Error: " +
                               std::string(std::strerror(errno)));
  }
  data_.read(reinterpret_cast<char*>(span.data()), span.size());
  if (!data_.good()) {
    return absl::InternalError("Failed to read from file. Error: " +
                               std::string(std::strerror(errno)));
  }
  return absl::OkStatus();
}

absl::Status FStreamFile::Write(std::size_t pos,
                                std::span<const std::byte> span) {
  // Grow file as needed.
  RETURN_IF_ERROR(GrowFileIfNeeded(pos + span.size()));
  data_.seekp(pos);
  if (!data_.good()) {
    return absl::InternalError("Failed to seek to position. Error: " +
                               std::string(std::strerror(errno)));
  }
  data_.write(reinterpret_cast<const char*>(span.data()), span.size());
  if (!data_.good()) {
    return absl::InternalError("Failed to write to file. Error: " +
                               std::string(std::strerror(errno)));
  }
  return absl::OkStatus();
}

absl::Status FStreamFile::Flush() {
  data_.flush();
  if (!data_.good()) {
    return absl::InternalError("Failed to flush file. Error: " +
                               std::string(std::strerror(errno)));
  }
  return absl::OkStatus();
}

absl::Status FStreamFile::Close() {
  if (!data_ || !data_.is_open()) {
    return absl::OkStatus();
  }
  RETURN_IF_ERROR(Flush());
  data_.close();
  if (!data_.good()) {
    return absl::InternalError("Failed to close file. Error: " +
                               std::string(std::strerror(errno)));
  }
  return absl::OkStatus();
}

absl::Status FStreamFile::GrowFileIfNeeded(std::size_t needed) {
  // Retain a 256 KiB buffer of zeros for initializing disk space.
  constexpr static std::size_t kStepSize = 1 << 18;
  static auto kZeros = std::make_unique<const std::array<char, kStepSize>>();
  if (file_size_ >= needed) {
    return absl::OkStatus();
  }
  data_.seekp(0, std::ios::end);
  if (!data_.good()) {
    return absl::InternalError("Failed to seek to end of file. Error: " +
                               std::string(std::strerror(errno)));
  }
  while (file_size_ < needed) {
    auto step = std::min(kStepSize, needed - file_size_);
    data_.write(kZeros->data(), step);
    if (!data_.good()) {
      return absl::InternalError("Failed to write to file. Error: " +
                                 std::string(std::strerror(errno)));
    }
    file_size_ += step;
  }
  return absl::OkStatus();
}

absl::StatusOr<CFile> CFile::Open(const std::filesystem::path& path) {
  // Create the parent directory.
  RETURN_IF_ERROR(CreateDirectory(path.parent_path()));
  // Append mode will create the file if it does not exist.
  auto file = std::fopen(path.string().c_str(), "a");
  if (file == nullptr) {
    return GetStatusWithSystemError(absl::StatusCode::kInternal,
                                    absl::StrFormat("Failed to open file %s", path.string()));
  }
  if (std::fclose(file) == EOF) {
    return GetStatusWithSystemError(absl::StatusCode::kInternal,
                                    absl::StrFormat("Failed to close file %s", path.string()));
  }
  // But for read/write we need the file to be opened in expended read mode.
  file = std::fopen(path.string().c_str(), "r+b");
  if (file == nullptr) {
    return GetStatusWithSystemError(absl::StatusCode::kInternal,
                                    absl::StrFormat("Failed to open file %s", path.string()));
  }
  // Seek to the end to get the file.
  if (std::fseek(file, 0, SEEK_END) != 0) {
    return GetStatusWithSystemError(absl::StatusCode::kInternal,
                                  absl::StrFormat("Failed to seek to end of file %s", path.string()));
  }
  // Get the file size.
  auto file_size = std::ftell(file);
  if (file_size == -1) {
    return GetStatusWithSystemError(absl::StatusCode::kInternal,
                                  absl::StrFormat("Failed to get size of file %s", path.string()));
  }
  return CFile(file, file_size);
}

CFile::CFile(std::FILE* file, std::size_t file_size) : file_size_(file_size), file_(file) {}

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
    return absl::InternalError("Failed to seek to position. Error: " +
                               std::string(std::strerror(errno)));
  }
  auto len = std::fread(span.data(), sizeof(std::byte), span.size(), file_);
  if (std::ferror(file_)) {
    return absl::InternalError("Failed to read from file. Error: " +
                               std::string(std::strerror(errno)));
  }
  if (len != span.size()) {
    return absl::InternalError(
        absl::StrFormat("Read different number of bytes than requested."
                        " Requested: %d, Read: %d",
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
    return absl::InternalError("Failed to seek to position. Error: " +
                               std::string(std::strerror(errno)));
  }
  auto len = std::fwrite(span.data(), sizeof(std::byte), span.size(), file_);
  if (std::ferror(file_)) {
    return absl::InternalError("Failed to write to file. Error: " +
                               std::string(std::strerror(errno)));
  }
  if (len != span.size()) {
    return absl::InternalError(
        absl::StrFormat("Wrote different number of bytes than requested."
                        " Requested: %d, Written: %d",
                        span.size(), len));
  }
  return absl::OkStatus();
}

absl::Status CFile::Flush() {
  if (file_ != nullptr && std::fflush(file_) == EOF) {
    return absl::InternalError("Failed to flush file. Error: " +
                               std::string(std::strerror(errno)));
  }
  return absl::OkStatus();
}

absl::Status CFile::Close() {
  if (file_ != nullptr) {
    RETURN_IF_ERROR(Flush());
    if (std::fclose(file_) == EOF) {
      return absl::InternalError("Failed to close file. Error: " +
                                 std::string(std::strerror(errno)));
    }
    file_ = nullptr;
  }
  return absl::OkStatus();
}

absl::Status CFile::GrowFileIfNeeded(std::size_t needed) {
  // Retain a 256 KiB buffer of zeros for initializing disk space.
  constexpr static std::size_t kStepSize = 1 << 18;
  static auto kZeros = std::make_unique<const std::array<char, kStepSize>>();
  if (file_size_ >= needed) {
    return absl::OkStatus();
  }
  if (std::fseek(file_, 0, SEEK_END) != 0) {
    return absl::InternalError("Failed to seek to end of file. Error: " +
                               std::string(std::strerror(errno)));
  }
  while (file_size_ < needed) {
    auto step = std::min(kStepSize, needed - file_size_);
    auto len = std::fwrite(kZeros->data(), sizeof(std::byte), step, file_);
    if (std::ferror(file_)) {
      return absl::InternalError("Failed to write to file. Error: " +
                                 std::string(std::strerror(errno)));
    }
    if (len != step) {
      return absl::InternalError(
          "Wrote different number of bytes than requested.");
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
            absl::StatusCode::kInternal,
            absl::StrFormat("Failed to open file %s.", path.string()));
  }
  // Seek to the end to get the file.
  off_t size = lseek(fd, 0, SEEK_END);
  if (size == -1) {
    return GetStatusWithSystemError(
            absl::StatusCode::kInternal,
            absl::StrFormat("Failed to seek to end of file %s.", path.string()));
  }
  return PosixFile(fd, size);
}

PosixFile::PosixFile(int fd, std::size_t file_size) : file_size_(file_size), fd_(fd) {}

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
    return absl::InternalError("Failed to seek to position. Error: " +
                               std::string(std::strerror(errno)));
  }
  auto len = read(fd_, span.data(), span.size());
  if (len == -1) {
    return absl::InternalError("Failed to read from file. Error: " +
                               std::string(std::strerror(errno)));
  }
  if (len != static_cast<ssize_t>(span.size())) {
    return absl::InternalError(
        absl::StrFormat("Read different number of bytes than requested."
                        " Requested: %d, Read: %d",
                        span.size(), len));
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
    return absl::InternalError("Failed to seek to position. Error: " +
                               std::string(std::strerror(errno)));
  }
  auto len = write(fd_, span.data(), span.size());
  if (len == -1) {
    return absl::InternalError("Failed to write to file. Error: " +
                               std::string(std::strerror(errno)));
  }
  if (len != static_cast<ssize_t>(span.size())) {
    return absl::InternalError(
        absl::StrFormat("Wrote different number of bytes than requested."
                        "Wrote %d, requested %d",
                        len, span.size()));
  }
  return absl::OkStatus();
}

absl::Status PosixFile::Flush() {
  if (fd_ >= 0 && fsync(fd_) == -1) {
    return absl::InternalError("Failed to flush file. Error: " +
                               std::string(std::strerror(errno)));
  }
  return absl::OkStatus();
}

absl::Status PosixFile::Close() {
  if (fd_ >= 0) {
    RETURN_IF_ERROR(Flush());
    if (close(fd_) == -1) {
      return absl::InternalError("Failed to close file. Error: " +
                                 std::string(std::strerror(errno)));
    }
    fd_ = -1;
  }
  return absl::OkStatus();
}

absl::Status PosixFile::GrowFileIfNeeded(std::size_t needed) {
  // Retain a 256 KiB buffer of zeros for initializing disk space.
  constexpr static std::size_t kStepSize = 1 << 18;
  static auto kZeros = std::make_unique<ArrayPage<int, kStepSize>>();
  if (file_size_ >= needed) {
    return absl::OkStatus();
  }
  auto offset = lseek(fd_, 0, SEEK_END);
  if (offset == -1) {
    return absl::InternalError("Failed to seek to end of file. Error: " +
                               std::string(std::strerror(errno)));
  }
  if (offset != static_cast<off_t>(file_size_)) {
    return absl::InternalError(
        absl::StrFormat("Failed to seek to end of file. Expected position: %d, "
                        "actual position: %d",
                        file_size_, offset));
  }
  while (file_size_ < needed) {
    auto step = std::min(kStepSize, needed - file_size_);
    auto len = write(fd_, kZeros->AsRawData().data(), step);
    if (len < 0) {
      return absl::InternalError("Failed to write to file. Error: " +
                                 std::string(std::strerror(errno)));
    }
    if (len != static_cast<ssize_t>(step)) {
      return absl::InternalError(
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
