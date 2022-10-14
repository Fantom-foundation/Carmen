#pragma once

#include <cstdint>

namespace carmen::backend {

// A PageId is used to identify a page within a file. Pages are to be indexed in
// sequence starting with 0. Thus, a page ID of 5 present in a file implicitly
// asserts the existence of pages 0-4 in the same file.
using PageId = std::size_t;

}  // namespace carmen::backend
