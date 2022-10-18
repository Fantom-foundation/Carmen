#pragma once

#include <array>
#include <concepts>
#include <span>

#include "common/type.h"

namespace carmen::backend {

// A Page is a fixed-size memory object that has a raw byte representation that
// can be used for loading and storing data in paged files.
template <typename P>
concept Page = requires(const P a, P b) {
  // Pages must support immutable and mutable raw data access.
  { a.AsRawData() } -> std::same_as<std::span<const std::byte, sizeof(P)>>;
  { b.AsRawData() } -> std::same_as<std::span<std::byte, sizeof(P)>>;
  // To be used in page pools, pages must also be default constructable and
  // destructible.
  std::is_default_constructible_v<P>;
  std::is_destructible_v<P>;
};

// A page containing an array of trivial values. As such, it is the in-memory,
// typed version of a page in a file containing a fixed length array of trivial
// elements. Furthermore, it provides index based access to the contained data.
//
// The trival type V is the type of value stored in this page, in the form of an
// array. The provided pages_size_in_byte is the number of bytes each page is
// comprising. Note that, if page_size_in_byte is not a multiple of sizeof(V)
// some extra bytes per page may be kept in memory and on disk.
template <Trivial V, std::size_t page_size_in_byte>
class ArrayPage final {
 public:
  // A constant defining the number of elements stored in each page of this
  // type.
  constexpr static std::size_t kNumElementsPerPage =
      page_size_in_byte / sizeof(V);

  // Provides read-only indexed access to a value in this page.
  const V& operator[](std::size_t pos) const {
    const V* data = reinterpret_cast<const V*>(&data_[0]);
    return data[pos];
  }

  // Provides mutable indexed access to a value in this page.
  V& operator[](std::size_t pos) {
    V* data = reinterpret_cast<V*>(&data_[0]);
    return data[pos];
  }

  // Provides read-only access to the raw data stored in this page. The intended
  // use is for storing data to disk and hashing the page's content.
  std::span<const std::byte, page_size_in_byte> AsRawData() const {
    return data_;
  }

  // Provides a mutable raw view of the data stored in this page. The main
  // intended use case is to replace the content when loading a page from disk.
  std::span<std::byte, page_size_in_byte> AsRawData() { return data_; }

 private:
  // Stores element's data in serialized form.
  std::array<std::byte, page_size_in_byte> data_;
};

}  // namespace carmen::backend
