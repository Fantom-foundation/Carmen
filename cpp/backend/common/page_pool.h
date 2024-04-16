/*
 * Copyright (c) 2024 Fantom Foundation
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at fantom.foundation/bsl11.
 *
 * Change Date: 2028-4-16
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by the GNU Lesser General Public Licence v3.
 */

#pragma once

#include <memory>
#include <optional>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "backend/common/eviction_policy.h"
#include "backend/common/file.h"
#include "backend/common/page.h"
#include "common/memory_usage.h"
#include "common/status_util.h"

namespace carmen::backend {

// ------------------------------- Declarations -------------------------------

template <std::size_t page_size>
class PagePoolListener;

// A PagePool implements a fixed sized in-memory cache of pages of a file. It is
// intended to be used in-between a File and a storage implementation to cache
// loaded data and aggregate write operations to individual pages.
//
// Each PagePool is backed by a file instance from which it fetches pages and to
// which it writes modifications to. Furthermore, listeners may be registered,
// enabling the injection of extra operations during page load and eviction
// steps.
template <File F, EvictionPolicy E = LeastRecentlyUsedEvictionPolicy>
class PagePool {
 public:
  using File = F;
  using Listener = PagePoolListener<F::kPageSize>;
  using EvictionPolicy = E;

  // Creates a pool backed by a default instance of the pools File
  // implementation.
  PagePool(std::size_t pool_size = 100000);

  // Creates a pool instance backed by the provided File.
  PagePool(std::unique_ptr<File> file, std::size_t pool_size = 100000);

  // Returns the maximum number of pages to be retained in this pool.
  std::size_t GetPoolSize() const { return pool_size_; }

  // Retrieves a reference to a page within this pool. If the page is present,
  // the existing page is returned. If the page is missing, it is fetched from
  // the disk. This may require the eviction of another page.
  // Note: the returned reference is only valid until the next Get() call.
  template <Page Page>
  StatusOrRef<Page> Get(PageId id);

  // Marks the given page as being modified. Thus, before it gets evicted from
  // the pool, it needs to be written back to the file.
  // TODO: find an implicit way to trace dirty pages
  void MarkAsDirty(PageId id);

  // Registers a page pool listener monitoring events.
  void AddListener(std::unique_ptr<Listener> listener);

  // Synchronizes all pages in the pool by writing all dirty pages out to disk.
  // Does not affect the content of the pool, nor are page accesses reported to
  // the pool policies.
  absl::Status Flush();

  // Flushes the content of this pool and released the underlying file.
  absl::Status Close();

  // Summarizes the memory usage of this instance.
  MemoryFootprint GetMemoryFootprint() const;

  // Returns the access to the utilized eviction policy, mainly for testing.
  EvictionPolicy& GetEvictionPolicy() { return eviction_policy_; }

 private:
  // Obtains a free slot in the pool. If all are occupied, a page is evicted to
  // make space.
  absl::StatusOr<std::size_t> GetFreeSlot();

  // Performs the eviction of a page at the given position.
  absl::Status EvictSlot(int position);

  // The file used for loading and storing pages.
  std::unique_ptr<File> file_;

  // The page pool, containing the actual data. It is using a pointer instead of
  // a vector because this makes its initialization faster (vector is
  // initializing each individual page).
  // TODO: test whether this is still fast during initialization.
  std::unique_ptr<RawPage<F::kPageSize>[]> pool_;

  // The number of pages in this pool.
  std::size_t pool_size_;

  // The employed eviction policy.
  EvictionPolicy eviction_policy_;

  // Flags indicating which entries in the page pool are dirty and need to be
  // stored to disk before being evicted.
  std::vector<bool> dirty_;

  // Indexes recording which page is in which pool position.
  absl::flat_hash_map<PageId, int> pages_to_index_;
  std::vector<PageId> index_to_pages_;

  // A list of free slots, used like a FIFO.
  std::vector<std::size_t> free_list_;

  // A list of listeners observing the page pool state.
  std::vector<std::unique_ptr<Listener>> listeners_;
};

// A PagePoolListener provides an observer interface to the activities within a
// PagePool. It is intended to be used for injecting operations on page load
// and/or evict operations.
template <std::size_t page_size>
class PagePoolListener {
 public:
  virtual ~PagePoolListener() {}
  // Called after a page got loaded from the file.
  virtual void AfterLoad(PageId id, const RawPage<page_size>& page) = 0;
  // Called before a page gets evicted from the page pool.
  virtual void BeforeEvict(PageId id, const RawPage<page_size>& page,
                           bool is_dirty) = 0;
};

// ------------------------------- Definitions --------------------------------

template <File F, EvictionPolicy E>
PagePool<F, E>::PagePool(std::size_t pool_size)
    : PagePool(std::make_unique<File>(), pool_size) {}

template <File F, EvictionPolicy E>
PagePool<F, E>::PagePool(std::unique_ptr<File> file, std::size_t pool_size)
    : file_(std::move(file)),
      pool_(new RawPage<F::kPageSize>[pool_size]),
      pool_size_(pool_size),
      eviction_policy_(pool_size) {
  dirty_.resize(pool_size);
  index_to_pages_.resize(pool_size);
  pages_to_index_.reserve(pool_size);
  free_list_.reserve(pool_size);
  for (std::size_t i = 0; i < pool_size; i++) {
    free_list_.push_back(pool_size - i - 1);
  }
}

template <File F, EvictionPolicy E>
template <Page Page>
StatusOrRef<Page> PagePool<F, E>::Get(PageId id) {
  // Try to locate the page in the pool first.
  auto pos = pages_to_index_.find(id);
  if (pos != pages_to_index_.end()) {
    eviction_policy_.Read(pos->second);
    return pool_[pos->second].template As<Page>();
  }

  // The page is missing, so we need to load it from disk.
  ASSIGN_OR_RETURN(auto idx, GetFreeSlot());
  Page& page = pool_[idx].template As<Page>();
  RETURN_IF_ERROR(file_->LoadPage(id, page));
  pages_to_index_[id] = idx;
  index_to_pages_[idx] = id;
  eviction_policy_.Read(idx);

  // Notify listeners about loaded page.
  for (auto& listener : listeners_) {
    listener->AfterLoad(id, pool_[idx]);
  }

  return page;
}

template <File F, EvictionPolicy E>
void PagePool<F, E>::MarkAsDirty(PageId id) {
  auto pos = pages_to_index_.find(id);
  if (pos != pages_to_index_.end()) {
    dirty_[pos->second] = true;
    eviction_policy_.Written(pos->second);
  }
}

template <File F, EvictionPolicy E>
void PagePool<F, E>::AddListener(std::unique_ptr<Listener> listener) {
  if (listener != nullptr) {
    listeners_.push_back(std::move(listener));
  }
}

template <File F, EvictionPolicy E>
absl::Status PagePool<F, E>::Flush() {
  if (!file_) {
    return absl::OkStatus();
  }
  for (std::size_t i = 0; i < pool_size_; i++) {
    if (!dirty_[i]) continue;
    RETURN_IF_ERROR(file_->StorePage(index_to_pages_[i], pool_[i]));
    dirty_[i] = false;
  }
  return absl::OkStatus();
}

template <File F, EvictionPolicy E>
absl::Status PagePool<F, E>::Close() {
  RETURN_IF_ERROR(Flush());
  if (file_) {
    RETURN_IF_ERROR(file_->Close());
  }
  return absl::OkStatus();
}

template <File F, EvictionPolicy E>
MemoryFootprint PagePool<F, E>::GetMemoryFootprint() const {
  MemoryFootprint res(*this);
  res.Add("pool", Memory(F::kPageSize * pool_size_));
  res.Add("dirty", Memory(dirty_.size() / 8 + 1));
  res.Add("pages_to_index", SizeOf(pages_to_index_));
  res.Add("index_to_pages", SizeOf(index_to_pages_));
  res.Add("free_list", SizeOf(free_list_));
  res.Add("listeners", SizeOf(listeners_));
  return res;
}

template <File F, EvictionPolicy E>
absl::StatusOr<std::size_t> PagePool<F, E>::GetFreeSlot() {
  // If there are unused pages, use those first.
  if (!free_list_.empty()) {
    std::size_t res = free_list_.back();
    free_list_.pop_back();
    return res;
  }

  // Let policy select the page to be evicted.
  auto trg = eviction_policy_.GetPageToEvict();

  // Fall-back: if policy can not decide, use a random page.
  if (!trg) {
    trg = rand() % pool_size_;
  }

  // Evict page to make space.
  RETURN_IF_ERROR(EvictSlot(*trg));
  return *trg;
}

template <File F, EvictionPolicy E>
absl::Status PagePool<F, E>::EvictSlot(int pos) {
  // Notify listeners about pending eviction.
  auto page_id = index_to_pages_[pos];
  bool is_dirty = dirty_[pos];
  for (auto& listener : listeners_) {
    listener->BeforeEvict(page_id, pool_[pos], is_dirty);
  }

  // Write to file if dirty.
  if (is_dirty) {
    RETURN_IF_ERROR(file_->StorePage(page_id, pool_[pos]));
    dirty_[pos] = false;
  }

  // Erase page ID association of slot.
  pages_to_index_.erase(page_id);
  eviction_policy_.Removed(pos);
  return absl::OkStatus();
}

}  // namespace carmen::backend
