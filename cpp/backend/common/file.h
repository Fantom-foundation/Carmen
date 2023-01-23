#pragma once

#include <concepts>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <deque>
#include <filesystem>
#include <fstream>
#include <span>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "backend/common/page.h"
#include "backend/common/page_id.h"
#include "common/fstream.h"
#include "common/status_util.h"

namespace carmen::backend {

// ------------------------------- Declarations -------------------------------

// Creates the provided directory file path recursively in case it does not
// exit. Returns true if the directory exists after the call, false otherwise.
absl::Status CreateDirectory(const std::filesystem::path& dir);

// Creates empty file at the provided file path. Returns ok status if the file
// was created successfully, otherwise returns the error status.
absl::Status CreateFile(const std::filesystem::path& path);

// The File concept defines an interface for file implementations supporting the
// loading and storing of fixed length pages. Pages are expected to be numbered
// in the range [0..n-1], where n is the number of pages in the file.
template <typename F>
concept File = requires(F a) {
  // Files must be movable.
  std::is_move_constructible_v<F>;
  std::is_move_assignable_v<F>;

  // All files must be open-able through a static factory function.
  {
    F::Open(std::declval<const std::filesystem::path&>())
    } -> std::same_as<absl::StatusOr<F>>;

  // Each file implementation must support the extraction of the number of
  // pages.
  { a.GetNumPages() } -> std::same_as<std::size_t>;
  // LoadPage is intended to be used for fetching a single page from the file.
  {
    a.LoadPage(PageId{}, std::declval<std::span<std::byte, F::kPageSize>>())
    } -> std::same_as<absl::Status>;
  // StorePage is intended to be used for fetching a single page from the file.
  {
    a.StorePage(PageId{},
                std::declval<std::span<const std::byte, F::kPageSize>>())
    } -> std::same_as<absl::Status>;
  // Each file has to support a flush operation after which data previously
  // written must be persisted on disk.
  { a.Flush() } -> std::same_as<absl::Status>;
  // Each file has to support a close operation, flushing buffered data and
  // releasing file resources. After a file is closed it may no longer be used.
  { a.Close() } -> std::same_as<absl::Status>;
};

// An InMemoryFile implement is provided to for testing purposes, where actual
// file operations are not relevant. It may also serve as a reference
// implementation to compare other implementations to in unit testing.
template <std::size_t page_size>
class InMemoryFile {
 public:
  constexpr static std::size_t kPageSize = page_size;

  static absl::StatusOr<InMemoryFile> Open(const std::filesystem::path&) {
    return InMemoryFile();
  }

  InMemoryFile() = default;

  std::size_t GetNumPages() const { return data_.size(); }

  absl::Status LoadPage(PageId id, std::span<std::byte, page_size> trg) const;

  absl::Status StorePage(PageId id, std::span<const std::byte, page_size> src);

  absl::Status Flush() const {
    // Nothing to do.
    return absl::OkStatus();
  }

  absl::Status Close() const {
    // Nothing to do.
    return absl::OkStatus();
  }

 private:
  using Block = std::array<std::byte, page_size>;
  std::deque<Block> data_;
};

namespace internal {

// A FStreamFile provides raw read/write access to a file through C++ streams.
// It provides a utility for implementing actual stricter typed File
// implementations. Note: FStreamFile is not satisfying any File concept.
class FStreamFile {
 public:
  // Opens the file at the provided path. If the file does not exist it will be
  // created.
  static absl::StatusOr<FStreamFile> Open(const std::filesystem::path& path);

  // Assure the file is move constructable.
  FStreamFile(FStreamFile&&) noexcept = default;

  // Flushes the content and closes the file.
  ~FStreamFile();

  // Provides the current file size in bytes.
  std::size_t GetFileSize() const;

  // Reads a range of bytes from the file to the given span. The provided
  // position is the starting position. The number of bytes to be read is taken
  // from the length of the provided span.
  absl::Status Read(std::size_t pos, std::span<std::byte> span);

  // Writes a span of bytes to the file at the given position. If needed, the
  // file is grown to fit all the data of the span. Additional bytes between the
  // current end and the starting position are initialized with zeros.
  absl::Status Write(std::size_t pos, std::span<const std::byte> span);

  // Flushes all pending/buffered writes to disk.
  absl::Status Flush();

  // Flushes the file and closes the underlying resource.
  absl::Status Close();

 private:
  FStreamFile(FStream fs, std::size_t file_size);

  // Grows the underlying file to the given size.
  absl::Status GrowFileIfNeeded(std::size_t needed);

  std::size_t file_size_;
  FStream data_;
};

// A CFile provides raw read/write access to a file C's stdio.h header.
class CFile {
 public:
  // Opens the given file in read/write mode. If it does not exist, the file is
  // created.
  static absl::StatusOr<CFile> Open(const std::filesystem::path& path);

  // Assure the file is move constructable.
  CFile(CFile&&) noexcept;

  // Flushes the content and closes the file.
  ~CFile();

  // Provides the current file size in bytes.
  std::size_t GetFileSize() const;

  // Reads a range of bytes from the file to the given span. The provided
  // position is the starting position. The number of bytes to be read is taken
  // from the length of the provided span.
  absl::Status Read(std::size_t pos, std::span<std::byte> span);

  // Writes a span of bytes to the file at the given position. If needed, the
  // file is grown to fit all the data of the span. Additional bytes between the
  // current end and the starting position are initialized with zeros.
  absl::Status Write(std::size_t pos, std::span<const std::byte> span);

  // Flushes all pending/buffered writes to disk.
  absl::Status Flush();

  // Flushes the file and closes the underlying resource.
  absl::Status Close();

 private:
  CFile(std::FILE* file, std::size_t file_size);

  // Grows the underlying file to the given size.
  absl::Status GrowFileIfNeeded(std::size_t needed);

  std::size_t file_size_;
  std::FILE* file_;
};

// A PosixFile provides raw read/write access to a file through POSIX API.
class PosixFile {
 public:
  // Opens the given file in read/write mode. If it does not exist, the file is
  // created.
  static absl::StatusOr<PosixFile> Open(const std::filesystem::path& path);

  // Assure the file is move constructable.
  PosixFile(PosixFile&&) noexcept;

  // Flushes the content and closes the file.
  ~PosixFile();

  // Provides the current file size in bytes.
  std::size_t GetFileSize() const;

  // Reads a range of bytes from the file to the given span. The provided
  // position is the starting position. The number of bytes to be read is taken
  // from the length of the provided span.
  absl::Status Read(std::size_t pos, std::span<std::byte> span);

  // Writes a span of bytes to the file at the given position. If needed, the
  // file is grown to fit all the data of the span. Additional bytes between the
  // current end and the starting position are initialized with zeros.
  absl::Status Write(std::size_t pos, std::span<const std::byte> span);

  // Flushes all pending/buffered writes to disk.
  absl::Status Flush();

  // Flushes the file and closes the underlying resource.
  absl::Status Close();

 private:
  PosixFile(int fd, std::size_t file_size);

  // Grows the underlying file to the given size.
  absl::Status GrowFileIfNeeded(std::size_t needed);

  std::size_t file_size_;
  int fd_;
};

}  // namespace internal

// An implementation of the File concept using a single file as a persistent
// storage solution.
template <std::size_t page_size, typename RawFile>
class SingleFileBase {
 public:
  constexpr static std::size_t kPageSize = page_size;

  static absl::StatusOr<SingleFileBase> Open(
      const std::filesystem::path& path) {
    ASSIGN_OR_RETURN(auto file, RawFile::Open(path));
    return SingleFileBase(std::move(file));
  }

  std::size_t GetNumPages() const { return file_.GetFileSize() / page_size; }

  absl::Status LoadPage(PageId id, std::span<std::byte, page_size> trg) const {
    return file_.Read(id * page_size, trg);
  }

  absl::Status StorePage(PageId id, std::span<const std::byte, page_size> src) {
    return file_.Write(id * page_size, src);
  }

  absl::Status Flush() { return file_.Flush(); }

  absl::Status Close() { return file_.Close(); }

 private:
  SingleFileBase(RawFile file) : file_(std::move(file)) {}

  mutable RawFile file_;
};

// Defines the default SingleFile format to use the C API.
// Client code like the FileIndex or FileStore depend on the file type exhibit a
// single template parameter. Thus, this alias definition here is required.
template <std::size_t page_size>
using SingleFile = SingleFileBase<page_size, internal::CFile>;

// ------------------------------- Definitions --------------------------------

template <std::size_t page_size>
absl::Status InMemoryFile<page_size>::LoadPage(
    PageId id, std::span<std::byte, page_size> trg) const {
  static const Block zero{};
  auto src = id >= data_.size() ? &zero : &data_[id];
  std::memcpy(trg.data(), src, page_size);
  return absl::OkStatus();
}

template <std::size_t page_size>
absl::Status InMemoryFile<page_size>::StorePage(
    PageId id, std::span<const std::byte, page_size> src) {
  while (data_.size() <= id) {
    data_.resize(id + 1);
  }
  std::memcpy(&data_[id], src.data(), page_size);
  return absl::OkStatus();
}

}  // namespace carmen::backend
