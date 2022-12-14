#pragma once

#include <array>
#include <concepts>
#include <span>

#include "common/type.h"

namespace carmen::backend {

// A constant defining the file system's page size.
constexpr static const std::size_t kFileSystemPageSize = 1 << 12;  // 4 KiB

// A Page is a fixed-size memory object that has a raw byte representation that
// can be used for loading and storing data in paged files.
template <typename P>
concept Page =
    // Pages must be aligned to a multiple of a file system page.
    alignof(P) >= kFileSystemPageSize &&
    alignof(P) % kFileSystemPageSize == 0 &&
    // To be used in page pools, pages must also be trivially default
    // constructible and destructible.
    std::is_trivially_default_constructible_v<
        P>&& std::is_trivially_destructible_v<P>&& requires(const P a, P b) {
  // Pages must support immutable and mutable raw data access.
  { a.AsRawData() } -> std::same_as<std::span<const std::byte, sizeof(P)>>;
  { b.AsRawData() } -> std::same_as<std::span<std::byte, sizeof(P)>>;
};

// Computes the required page size based on a use case specific needed page
// size. The required page size is the smallest multiple of the file system's
// page size that can fit the provided needed page size.
constexpr std::size_t GetRequiredPageSize(std::size_t needed_page_size) {
  // If the needed size is negative, zero, or less than a single file system
  // page, a single page is used.
  if (needed_page_size <= kFileSystemPageSize) {
    return kFileSystemPageSize;
  }
  // Otherwise, the size requirement is rounded up to the next full page.
  return needed_page_size % kFileSystemPageSize == 0
             ? needed_page_size
             : (((needed_page_size / kFileSystemPageSize) + 1) *
                kFileSystemPageSize);
}

// A page containing an array of trivial values. As such, it is the in-memory,
// typed version of a page in a file containing a fixed length array of trivial
// elements. Furthermore, it provides index based access to the contained data.
//
// The trival type V is the type of value stored in this page, in the form of an
// array. The provided pages_size_in_byte is the number of bytes each page is
// comprising. Note that, if page_size_in_byte is not a multiple of sizeof(V)
// some extra bytes per page may be kept in memory and on disk.
template <Trivial V, std::size_t page_size_in_byte = kFileSystemPageSize>
class alignas(kFileSystemPageSize) ArrayPage final {
 public:
  // A constant providing the full size of this page in memory and on disk.
  // Note that due to alignment constraints, this may exceed the specified
  // page_size_in_byte.
  constexpr static std::size_t full_page_size_in_byte =
      GetRequiredPageSize(page_size_in_byte);

  // A constant defining the number of elements stored in each page of this
  // type.
  constexpr static std::size_t kNumElementsPerPage =
      page_size_in_byte / sizeof(V);

  // Provides read-only indexed access to a value in this page.
  const V& operator[](std::size_t pos) const { return AsArray()[pos]; }

  // Provides mutable indexed access to a value in this page.
  V& operator[](std::size_t pos) { return AsArray()[pos]; }

  // Provides direct access to the stored array.
  const std::array<V, kNumElementsPerPage>& AsArray() const {
    return *reinterpret_cast<const std::array<V, kNumElementsPerPage>*>(&data_);
  }

  // Provides direct const access to the stored array.
  std::array<V, kNumElementsPerPage>& AsArray() {
    return *reinterpret_cast<std::array<V, kNumElementsPerPage>*>(&data_);
  }

  // Provides read-only access to the raw data stored in this page. The intended
  // use is for storing data to disk.
  std::span<const std::byte, full_page_size_in_byte> AsRawData() const {
    return data_;
  }

  // Provides a mutable raw view of the data stored in this page. The main
  // intended use case is to replace the content when loading a page from disk.
  std::span<std::byte, full_page_size_in_byte> AsRawData() { return data_; }

 private:
  // Stores element's data in serialized form.
  std::array<std::byte, full_page_size_in_byte> data_;
};

}  // namespace carmen::backend
