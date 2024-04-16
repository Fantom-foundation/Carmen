/*
 * Copyright (c) 2024 Fantom Foundation
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE file and at fantom.foundation/bsl11.
 *
 * Change Date: 2028-4-16
 *
 * On the date above, in accordance with the Business Source License, use of
 * this software will be governed by the GNU Lesser General Public Licence v3.
 */

#pragma once

#include <array>
#include <concepts>
#include <span>
#include <type_traits>

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
    // constructable and destructible.
    std::is_trivially_default_constructible_v<P>&& std::
        is_trivially_destructible_v<P>&& std::is_convertible_v<
            P, std::span<std::byte, sizeof(P)>>&& std::
            is_convertible_v<const P, std::span<const std::byte, sizeof(P)>>;

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

// A raw page is the simplest page format comprising a fixed-length array of
// bytes. It implements all page requirements and is used as a type-erased
// stand-in for generic page handling. Through its As<T>() member functions, a
// raw page may be interpreted as any specialized page.
template <std::size_t page_size = kFileSystemPageSize>
class alignas(kFileSystemPageSize) RawPage final {
 public:
  // Can be used to interpret the content of this page using the given page
  // format. It is a readability wrapper around a static cast, performing no
  // dynamic checks on the validity of the cast.
  template <Page Page>
  const Page& As() const {
    return const_cast<RawPage&>(*this).As<Page>();
  }

  // Same as above, but for mutable instances.
  template <Page Page>
  Page& As() {
    static_assert(sizeof(Page) == sizeof(RawPage));
    return reinterpret_cast<Page&>(*this);
  }

  // Provides read-only indexed access to a value in this page.
  std::byte operator[](std::size_t pos) const { return data_[pos]; }

  // Provides mutable indexed access to a value in this page.
  std::byte& operator[](std::size_t pos) { return data_[pos]; }

  // Provides read-only access to the raw data stored in this page. The intended
  // use is for storing data to disk.
  operator std::span<const std::byte, page_size>() const { return data_; }

  // Provides a mutable raw view of the data stored in this page. The main
  // intended use case is to replace the content when loading a page from disk.
  operator std::span<std::byte, page_size>() { return data_; }

 private:
  std::array<std::byte, page_size> data_;
};

// A page containing an array of trivial values. As such, it is the in-memory,
// typed version of a page in a file containing a fixed length array of trivial
// elements. Furthermore, it provides index based access to the contained data.
//
// The trivial type V is the type of value stored in this page, in the form of
// an array. The provided num_elements is the number values per page. Note that,
// if total size of the array is not a multiple of kFileSystemPageSize some
// extra bytes per page may be kept in memory and on disk.
template <Trivial V, std::size_t num_elements = kFileSystemPageSize / sizeof(V)>
class alignas(kFileSystemPageSize) ArrayPage final {
 public:
  // A constant providing the full size of this page in memory and on disk.
  // Note that due to alignment constraints, this may exceed the size of the
  // specified num_elements.
  constexpr static std::size_t full_page_size_in_byte =
      GetRequiredPageSize(num_elements * sizeof(V));

  // A constant defining the number of elements stored in each page of this
  // type.
  constexpr static std::size_t kNumElementsPerPage = num_elements;

  // Provides read-only indexed access to a value in this page.
  const V& operator[](std::size_t pos) const { return AsArray()[pos]; }

  // Provides mutable indexed access to a value in this page.
  V& operator[](std::size_t pos) { return AsArray()[pos]; }

  // Provides direct access to the stored array.
  const std::array<V, kNumElementsPerPage>& AsArray() const {
    return *reinterpret_cast<const std::array<V, kNumElementsPerPage>*>(this);
  }

  // Provides direct const access to the stored array.
  std::array<V, kNumElementsPerPage>& AsArray() {
    return *reinterpret_cast<std::array<V, kNumElementsPerPage>*>(this);
  }

  // Provides span access to the underlying array.
  operator std::span<const V, num_elements>() const { return AsArray(); }

  // Provides span access to the underlying array.
  operator std::span<V, num_elements>() { return AsArray(); }

  // Provides read-only access to the raw data stored in this page. The intended
  // use is for storing data to disk.
  operator std::span<const std::byte, full_page_size_in_byte>() const {
    return data_;
  }

  // Provides a mutable raw view of the data stored in this page. The main
  // intended use case is to replace the content when loading a page from disk.
  operator std::span<std::byte, full_page_size_in_byte>() { return data_; }

 private:
  // Stores element's data in serialized form.
  std::array<std::byte, full_page_size_in_byte> data_;
};

}  // namespace carmen::backend
