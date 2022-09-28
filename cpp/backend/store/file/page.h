#pragma once

#include <array>

#include "common/hash.h"

namespace carmen::backend::store {

// A PageId is used to identify a page within a file. Pages are to be indexed in
// sequence starting with 0. Thus, a page ID of 5 present in a file implicitly
// asserts the existence of pages 0-4 in the same file.
using PageId = std::size_t;

// The in-memory, typed version of a page in a file. It retains an in-memory
// copy of the binary data stored in the corresponding page of a file.
// Furthermore, it provides index based access to the contained data.
//
// The trival type V is the type of value stored in this page, in the form of an
// array. The provided pages_size_in_byte is the number of bytes each page is
// comprising. Note that, if page_size_in_byte is not a multiple of sizeof(V)
// some extra bytes per page may be kept in memory and on disk.
template <Trivial V, unsigned page_size_in_byte>
class Page final {
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

// A utility constant to get the number of elements of a given type in a page of
// a given size.
template <Trivial V, unsigned page_size_in_byte>
constexpr std::size_t kNumElementsPerPage =
    Page<V, page_size_in_byte>::kNumElementsPerPage;

}  // namespace carmen::backend::store
