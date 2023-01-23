#pragma once

#include <filesystem>
#include <fstream>
#include <span>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"

namespace carmen {

// A wrapper around std::fstream that provides error handling. This class is
// intended to be used instead of std::fstream directly.
class FStream {
 public:
  // Opens a file at the given path with the given mode. Returns an error if the
  // file could not be opened.
  static absl::StatusOr<FStream> Open(const std::filesystem::path& path,
                                      std::ios::openmode mode);

  // Reads the number of elements from file specified by size of the buffer.
  // Returns an error if read failed.
  template <typename T>
  absl::Status Read(std::span<T> buffer);

  // Reads a value of given type from the file.
  // Returns an error if read failed.
  template <typename T>
  absl::Status Read(T& buffer);

  // Reads the number of elements from file specified by size of the buffer.
  // When the end of the file is reached, the eof flag is swallowed. Returns
  // number of elements read. Returns an error if read failed.
  template <typename T>
  absl::StatusOr<std::size_t> ReadUntilEof(std::span<T> buffer);

  // Writes the number of elements to file specified by size of the buffer.
  // Returns an error if write failed.
  template <typename T>
  absl::Status Write(std::span<const T> data);

  // Writes value of given type into the file.
  // Returns an error if write failed.
  template <typename T>
  absl::Status Write(const T& data);

  // Seek to the given offset in the file. Should be used when reading from file
  // at certain position. Returns an error if seekg failed.
  absl::Status Seekg(std::size_t offset, std::ios::seekdir dir = std::ios::beg);

  // Get the current position in the file. Should be used when reading from
  // file. Returns an error if tellg failed.
  absl::StatusOr<std::size_t> Tellg();

  // Seek to the given offset in the file. Should be used when writing to file
  // at certain position. Returns an error if seekp failed.
  absl::Status Seekp(std::size_t offset, std::ios::seekdir dir = std::ios::beg);

  // Get the current position in the file. Should be used when writing to file.
  // Returns an error if tellp failed.
  absl::StatusOr<std::size_t> Tellp();

  // Flush the file. Returns an error if the flush failed.
  absl::Status Flush();

  // Close the file. Returns an error if the close failed.
  absl::Status Close();

  // Check if the file is open.
  bool IsOpen() const;

 private:
  FStream(std::fstream&& fs, std::filesystem::path path)
      : fs_(std::move(fs)), path_(std::move(path)) {}

  std::fstream fs_;
  std::filesystem::path path_;
};

template <typename T>
absl::Status FStream::Read(std::span<T> buffer) {
  fs_.read(reinterpret_cast<char*>(buffer.data()), buffer.size() * sizeof(T));
  if (fs_.good()) return absl::OkStatus();
  return absl::InternalError(
      absl::StrFormat("Failed to read from file %s.", path_.string()));
}

template <typename T>
absl::Status FStream::Read(T& buffer) {
  fs_.read(reinterpret_cast<char*>(&buffer), sizeof(T));
  if (fs_.good()) return absl::OkStatus();
  return absl::InternalError(
      absl::StrFormat("Failed to read from file %s.", path_.string()));
}

template <typename T>
absl::StatusOr<std::size_t> FStream::ReadUntilEof(std::span<T> buffer) {
  // Reading from closed file returns same flags as reading until eof, so we
  // need to check if the file is open before reading
  if (!fs_.is_open()) {
    return absl::InternalError(
        absl::StrFormat("Failed to read from file %s.", path_.string()));
  }
  fs_.read(reinterpret_cast<char*>(buffer.data()), buffer.size() * sizeof(T));
  // clear the eof flag (on eof the eof flag is set and the fail flag is set)
  if (fs_.eof()) {
    fs_.clear(fs_.rdstate() & ~(std::ios::eofbit | std::ios::failbit));
  }
  if (fs_.good()) return fs_.gcount() / sizeof(T);
  return absl::InternalError(
      absl::StrFormat("Failed to read from file %s.", path_.string()));
}

template <typename T>
absl::Status FStream::Write(std::span<const T> data) {
  fs_.write(reinterpret_cast<const char*>(data.data()),
            data.size() * sizeof(T));
  if (fs_.good()) return absl::OkStatus();
  return absl::InternalError(
      absl::StrFormat("Failed to write into file %s.", path_.string()));
}

template <typename T>
absl::Status FStream::Write(const T& data) {
  fs_.write(reinterpret_cast<const char*>(&data), sizeof(T));
  if (fs_.good()) return absl::OkStatus();
  return absl::InternalError(
      absl::StrFormat("Failed to write into file %s.", path_.string()));
}
}  // namespace carmen
