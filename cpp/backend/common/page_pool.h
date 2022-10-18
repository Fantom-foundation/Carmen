#pragma once

#include <memory>
#include <optional>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "backend/common/file.h"

namespace carmen::backend {

// ------------------------------- Declarations -------------------------------

template <typename P>
class PagePoolListener;

// A PagePool implements a fixed sized in-memory cache of pages of a file. It is
// intended to be used in-between a File and a storage implementation to cache
// loaded data and aggregate write operations to individual pages.
//
// Each PagePool is backed by a file instance from which it fetches pages and to
// which it writes modifications to. Furthermore, listeners may be registered,
// enabling the incjection of extra operations during page load and eviction
// steps.
template <typename P, template <typename> class F>
requires File<F<P>>
class PagePool {
 public:
  using Page = P;
  using File = F<Page>;
  using Listener = PagePoolListener<P>;

  // Creates a pool backed by a default instance of the pools File
  // implementation.
  PagePool(std::size_t pool_size = 100);

  // Creates a pool instance backed by the provided File.
  PagePool(std::unique_ptr<File> file, std::size_t pool_size = 100);

  // Returns the maximum number of pages to be retained in this pool.
  std::size_t GetPoolSize() const { return pool_.size(); }

  // Retrieves a reference to a page within this pool. If the page is present,
  // the existing page is returned. If the page is missing, it is fetched from
  // the disk. This may require the eviction of another page.
  // Note: the returned reference is only valid until the next Get() call.
  Page& Get(PageId id);

  // Marks the given page as being modified. Thus, before it gets evicted from
  // the pool, it needs to be written back to the file.
  // TODO: find an implicit way to trace dirty pages
  void MarkAsDirty(PageId id);

  // Registers a page pool listener monitoring events.
  void AddListener(std::unique_ptr<Listener> listener);

 private:
  // Obtains a free slot in the pool. If all are occupied, a page is evicted to
  // make space.
  std::size_t GetFreeSlot();

  // Performs the eviction of a page at the given position.
  void EvictSlot(int position);

  // The file used for loading and storing pages.
  std::unique_ptr<File> file_;

  // The page pool, containing the actual data.
  std::vector<Page> pool_;

  // Flags indicating which entries in the page pool are dirty and need to be
  // stored to disk before being evicted.
  std::vector<bool> dirty_;

  // Indexes recording which page is in which pool postion.
  absl::flat_hash_map<PageId, int> pages_to_index_;
  std::vector<std::optional<PageId>> index_to_pages_;

  // A list of listeners observing the page pool state.
  std::vector<std::unique_ptr<Listener>> listeners_;
};

// A PagePoolListener provides an observer interface to the activities within a
// PagePool. It is intended to be used for injecting operations on page load
// and/or evict operations.
template <typename P>
class PagePoolListener {
 public:
  using Page = P;
  virtual ~PagePoolListener() {}
  // Called after a page got loaded from the file.
  virtual void AfterLoad(PageId id, const Page& page) = 0;
  // Called before a page gets evicted from the page pool.
  virtual void BeforeEvict(PageId id, const Page& page, bool is_dirty) = 0;
};

// ------------------------------- Definitions --------------------------------

template <typename P, template <typename> class F>
requires File<F<P>> PagePool<P, F>::PagePool(std::size_t pool_size)
    : PagePool(std::make_unique<File>(), pool_size) {}

template <typename P, template <typename> class F>
requires File<F<P>> PagePool<P, F>::PagePool(std::unique_ptr<File> file,
                                             std::size_t pool_size)
    : file_(std::move(file)) {
  pool_.resize(pool_size);
  dirty_.resize(pool_size);
  index_to_pages_.resize(pool_size);
}

template <typename P, template <typename> class F>
requires File<F<P>>
typename PagePool<P, F>::Page& PagePool<P, F>::Get(PageId id) {
  // Try to locate the page in the pool first.
  auto pos = pages_to_index_.find(id);
  if (pos != pages_to_index_.end()) {
    return pool_[pos->second];
  }

  // The page is missing, so we need to load it from disk.
  auto idx = GetFreeSlot();
  Page& page = pool_[idx];
  file_->LoadPage(id, page);
  pages_to_index_[id] = idx;
  index_to_pages_[idx] = id;

  // Notify listeners about loaded page.
  for (auto& listener : listeners_) {
    listener->AfterLoad(id, page);
  }

  return page;
}

template <typename P, template <typename> class F>
requires File<F<P>>
void PagePool<P, F>::MarkAsDirty(PageId id) {
  auto pos = pages_to_index_.find(id);
  if (pos != pages_to_index_.end()) {
    dirty_[pos->second] = true;
  }
}

template <typename P, template <typename> class F>
requires File<F<P>>
void PagePool<P, F>::AddListener(std::unique_ptr<Listener> listener) {
  if (listener != nullptr) {
    listeners_.push_back(std::move(listener));
  }
}

template <typename P, template <typename> class F>
requires File<F<P>> std::size_t PagePool<P, F>::GetFreeSlot() {
  // TODO: make this more efficient.

  // Look for a free slot.
  for (std::size_t i = 0; i < index_to_pages_.size(); i++) {
    if (index_to_pages_[i] == std::nullopt) {
      return i;
    }
  }

  // Next, look for a clean page.
  for (std::size_t i = 0; i < dirty_.size(); i++) {
    if (!dirty_[i]) {
      EvictSlot(i);
      return i;
    }
  }

  // Evict a random page to make space.
  auto trg = rand() % pool_.size();
  EvictSlot(trg);
  return trg;
}

template <typename P, template <typename> class F>
requires File<F<P>>
void PagePool<P, F>::EvictSlot(int pos) {
  // Test whether slot is actually occupied.
  auto page_id = index_to_pages_[pos];
  if (page_id == std::nullopt) {
    return;
  }

  // Notify listeners about pending eviction.
  bool is_dirty = dirty_[pos];
  for (auto& listener : listeners_) {
    listener->BeforeEvict(*page_id, pool_[pos], is_dirty);
  }

  // Write to file if dirty.
  if (is_dirty) {
    file_->StorePage(*page_id, pool_[pos]);
    dirty_[pos] = false;
  }

  // Mark pool position as free.
  pages_to_index_.erase(*page_id);
  index_to_pages_[pos] = std::nullopt;
}

}  // namespace carmen::backend
