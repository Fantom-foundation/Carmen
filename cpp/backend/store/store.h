#pragma once

#include <span>

#include "backend/common/page_id.h"

namespace carmen::backend::store {

// A snapshot of the state of a store providing access to the contained data
// frozen at it creation time. This definies an interface for store
// implementation specific implementations.
//
// The life cycle of a snapshot defines the duration of its availability.
// Snapshots are volatile, thus not persistent over application restarts. A
// snapshot is created by a call to `CreateSnapshot()` on a store instance, and
// destroyed upon destruction. It does not (need) to persist beyond the lifetime
// of the current process.
class StoreSnapshot {
 public:
  virtual ~StoreSnapshot() {}

  // The total number of pages captured by this snapshot.
  virtual std::size_t GetNumPages() const = 0;

  // Gains read access to an individual page in the range [0,..,GetNumPages()).
  // The provided page data is only valid until the next call to this function
  // or destruction of the snapshot.
  virtual std::span<const std::byte> GetPageData(PageId) const = 0;
};

}  // namespace carmen::backend::store
