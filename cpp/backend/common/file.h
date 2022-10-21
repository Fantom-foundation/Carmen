#pragma once

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <deque>
#include <filesystem>
#include <fstream>
#include <span>

#include "backend/common/page.h"
#include "backend/common/page_id.h"

namespace carmen::backend {

// ------------------------------- Declarations -------------------------------

// The File concept defines an interface for file implementations supporting the
// loading and storing of fixed length pages. Pages are expected to be numbered
// in the range [0..n-1], where n is the number of pages in the file.
template <typename F>
concept File = requires(F a) {
  // Files must expose the page type.
  typename F::page_type;

  // Files must be movable.
  std::is_move_constructible_v<F>;
  std::is_move_assignable_v<F>;

  // Each file implementation must support the extraction of the number of
  // pages.
  { a.GetNumPages() } -> std::same_as<std::size_t>;
  // LoadPage is intended to be used for fetching a single page from the file.
  {
    a.LoadPage(PageId{}, const_cast<typename F::page_type&>(
                             std::declval<typename F::page_type&>()))
    } -> std::same_as<void>;
  // StorePage is intended to be used for fetching a single page from the file.
  {
    a.StorePage(PageId{}, std::declval<typename F::page_type>())
    } -> std::same_as<void>;
  // Each file has to support a flush operation after which data previously
  // written must be persisted on disk.
  { a.Flush() } -> std::same_as<void>;
  // Each file has to support a close operation, flushing buffered data and
  // releasing file resources. After a file is closed it may no longer be used.
  { a.Close() } -> std::same_as<void>;
};

// An InMemoryFile implement is provided to for testing purposes, where actual
// file operations are not relevant. It may also serve as a reference
// implementation to compare other implementations to in unit testing.
template <Page Page>
class InMemoryFile {
 public:
  using page_type = Page;

  InMemoryFile() = default;
  InMemoryFile(std::filesystem::path){};

  std::size_t GetNumPages() const { return data_.size(); }

  void LoadPage(PageId id, Page& trg) const;

  void StorePage(PageId id, const Page& src);

  void Flush() const {
    // Nothing to do.
  }

  void Close() const {
    // Nothing to do.
  }

 private:
  using Block = std::array<std::byte, sizeof(Page)>;
  std::deque<Block> data_;
};

namespace internal {

// A FStreamFile provides raw read/write access to a file through C++ streams.
// It provides a utility for implementing actual stricter typed File
// implementations. Note: FStreamFile is not satisfying any File concept.
class FStreamFile {
 public:
  // Opens the given file in read/write mode. If it does not exist, the file is
  // created. TODO(herbertjordan): add error handling.
  FStreamFile(std::filesystem::path file);
  // Flushes the content and closes the file.
  ~FStreamFile();

  // Provides the current file size in bytes.
  std::size_t GetFileSize();

  // Reads a range of bytes from the file to the given span. The provided
  // position is the starting position. The number of bytes to be read is taken
  // from the length of the provided span.
  void Read(std::size_t pos, std::span<std::byte> span);

  // Writes a span of bytes to the file at the given position. If needed, the
  // file is grown to fit all the data of the span. Additional bytes between the
  // current end and the starting position are initialized with zeros.
  void Write(std::size_t pos, std::span<const std::byte> span);

  // Flushes all pending/buffered writes to disk.
  void Flush();

  // Flushes the file and closes the underlying resource.
  void Close();

 private:
  // Grows the underlying file to the given size.
  void GrowFileIfNeeded(std::size_t needed);

  std::size_t file_size_;
  std::fstream data_;
};

// A CFile provides raw read/write access to a file C's stdio.h header.
class CFile {
 public:
  // Opens the given file in read/write mode. If it does not exist, the file is
  // created. TODO(herbertjordan): add error handling.
  CFile(std::filesystem::path file);
  // Flushes the content and closes the file.
  ~CFile();

  // Provides the current file size in bytes.
  std::size_t GetFileSize();

  // Reads a range of bytes from the file to the given span. The provided
  // position is the starting position. The number of bytes to be read is taken
  // from the length of the provided span.
  void Read(std::size_t pos, std::span<std::byte> span);

  // Writes a span of bytes to the file at the given position. If needed, the
  // file is grown to fit all the data of the span. Additional bytes between the
  // current end and the starting position are initialized with zeros.
  void Write(std::size_t pos, std::span<const std::byte> span);

  // Flushes all pending/buffered writes to disk.
  void Flush();

  // Flushes the file and closes the underlying resource.
  void Close();

 private:
  // Grows the underlying file to the given size.
  void GrowFileIfNeeded(std::size_t needed);

  std::size_t file_size_;
  std::FILE* file_;
};

// A PosixFile provides raw read/write access to a file through POSIX API.
class PosixFile {
 public:
  // Opens the given file in read/write mode. If it does not exist, the file is
  // created. TODO(herbertjordan): add error handling.
  PosixFile(std::filesystem::path file);
  // Flushes the content and closes the file.
  ~PosixFile();

  // Provides the current file size in bytes.
  std::size_t GetFileSize();

  // Reads a range of bytes from the file to the given span. The provided
  // position is the starting position. The number of bytes to be read is taken
  // from the length of the provided span.
  void Read(std::size_t pos, std::span<std::byte> span);

  // Writes a span of bytes to the file at the given position. If needed, the
  // file is grown to fit all the data of the span. Additional bytes between the
  // current end and the starting position are initialized with zeros.
  void Write(std::size_t pos, std::span<const std::byte> span);

  // Flushes all pending/buffered writes to disk.
  void Flush();

  // Flushes the file and closes the underlying resource.
  void Close();

 private:
  // Grows the underlying file to the given size.
  void GrowFileIfNeeded(std::size_t needed);

  std::size_t file_size_;
  int fd_;
};

}  // namespace internal

// An implementation of the File concept using a single file as a persistent
// storage solution.
template <Page Page>
class SingleFile {
 public:
  using page_type = Page;

  SingleFile(std::filesystem::path file_path) : file_(file_path) {}

  std::size_t GetNumPages() const { return file_.GetFileSize() / sizeof(Page); }

  void LoadPage(PageId id, Page& trg) const {
    file_.Read(id * sizeof(Page), trg.AsRawData());
  }

  void StorePage(PageId id, const Page& src) {
    file_.Write(id * sizeof(Page), src.AsRawData());
  }

  void Flush() { file_.Flush(); }

  void Close() { file_.Close(); }

 private:
  // mutable internal::FStreamFile file_;
  // mutable internal::CFile file_;
  mutable internal::PosixFile file_;
};

// ------------------------------- Definitions --------------------------------

template <Page Page>
void InMemoryFile<Page>::LoadPage(PageId id, Page& trg) const {
  static const Block zero{};
  auto src = id >= data_.size() ? &zero : &data_[id];
  std::memcpy(&trg, src, sizeof(Page));
}

template <Page Page>
void InMemoryFile<Page>::StorePage(PageId id, const Page& src) {
  while (data_.size() <= id) {
    data_.resize(id + 1);
  }
  std::memcpy(&data_[id], &src, sizeof(Page));
}

}  // namespace carmen::backend
