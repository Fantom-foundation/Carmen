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

#include "absl/status/statusor.h"
#include "backend/common/page.h"
#include "backend/common/page_id.h"
#include "backend/common/page_pool.h"
#include "common/status_util.h"

namespace carmen::backend {

// A page manager is like a memory manager organizing the life cycle of pages in
// a single file, accessed through a page pool. It allows to create (=allocate)
// new pages, resolve PageIDs to Pages (=dereferencing), and the freeing and
// reusing of pages.
//
// NOTE: this is still work in progress; missing features:
//  - free lists, for releasing and re-using pages
//  - support for serializing the page managers state
//  - support for computing the managers memory footprint
//  - pinning of pages
//
template <typename PagePool>
class PageManager {
 public:
  PageManager(PagePool pool = PagePool{}, PageId next = 0)
      : next_(next), pool_(std::move(pool)) {}

  // The type returned when allocating a new page, including the new page's id
  // and a reference to the new page.
  template <Page Page>
  struct NewPage {
    operator Page&() { return page; }
    operator PageId() { return id; }
    PageId id;
    Page& page;
  };

  // Creates a new page and returns the new page's ID and a page reference.
  template <Page Page>
  absl::StatusOr<NewPage<Page>> New() {
    PageId id = next_++;
    ASSIGN_OR_RETURN(Page & page, pool_.template Get<Page>(id));
    return NewPage<Page>{id, page};
  }

  // Resolves a page ID to a page reference. It is the task of the caller to
  // ensure the consistent usage of page types.
  template <Page Page>
  StatusOrRef<Page> Get(PageId id) const {
    return pool_.template Get<Page>(id);
  }

  // Marks the given page as dirty (=modified), indicating that it needs to be
  // written back to the disk before being evicted or during a flush.
  void MarkAsDirty(PageId id) { pool_.MarkAsDirty(id); }

  // Flushes the content of all managed pages to disk.
  absl::Status Flush() { return pool_.Flush(); }

  // Closes the underlying pool manager after flushing its content.
  absl::Status Close() { return pool_.Close(); }

 private:
  // The next page ID to be used for allocating a page.
  PageId next_;

  // The underlying page pool managing the actual file accesses.
  mutable PagePool pool_;
};

}  // namespace carmen::backend
