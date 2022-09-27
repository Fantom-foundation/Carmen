#pragma once

#include <memory>
#include <optional>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "backend/store/file/file.h"
#include "backend/store/file/page.h"

namespace carmen::backend::store {

template <Trivial V, template <std::size_t> class F, std::size_t page_size>
requires File<F<page_size>, page_size>
class PagePool {
 public:
  using File = F<page_size>;
  using Page = Page<V, page_size>;

  PagePool(std::size_t pool_size = 10);
  PagePool(std::unique_ptr<File> file, std::size_t pool_size = 10);

  std::size_t GetPoolSize() const { return pool_.size(); }
  Page& Get(PageId id);

  // TODO: find an implicit way to trace dirty pages
  void MarkAsDirty(PageId id);

 private:
  std::size_t GetFreeSlot();
  void EvictSlot(int position);

  std::unique_ptr<File> file_;
  std::vector<Page> pool_;
  std::vector<bool> dirty_;
  absl::flat_hash_map<PageId, int> pages_to_index_;
  std::vector<std::optional<PageId>> index_to_pages_;
};

template <Trivial V, template <std::size_t> class F, std::size_t page_size>
requires File<F<page_size>, page_size> PagePool<V, F, page_size>::PagePool(
    std::size_t pool_size)
    : PagePool(std::make_unique<File>(), pool_size) {}

template <Trivial V, template <std::size_t> class F, std::size_t page_size>
requires File<F<page_size>, page_size> PagePool<V, F, page_size>::PagePool(
    std::unique_ptr<File> file, std::size_t pool_size)
    : file_(std::move(file)) {
  pool_.resize(pool_size);
  dirty_.resize(pool_size);
  index_to_pages_.resize(pool_size);
}

template <Trivial V, template <std::size_t> class F, std::size_t page_size>
requires File<F<page_size>, page_size>
typename PagePool<V, F, page_size>::Page& PagePool<V, F, page_size>::Get(
    PageId id) {
  // Try to locate the page in the pool first.
  auto pos = pages_to_index_.find(id);
  if (pos != pages_to_index_.end()) {
    return pool_[pos->second];
  }

  // The page is missing, so we need to load it from disk.
  auto idx = GetFreeSlot();
  Page& page = pool_[idx];
  file_->LoadPage(id, page.AsRawData());
  pages_to_index_[id] = idx;
  index_to_pages_[idx] = id;
  return page;
}

template <Trivial V, template <std::size_t> class F, std::size_t page_size>
requires File<F<page_size>, page_size>
void PagePool<V, F, page_size>::MarkAsDirty(PageId id) {
  auto pos = pages_to_index_.find(id);
  if (pos != pages_to_index_.end()) {
    dirty_[pos->second] = true;
  }
}

template <Trivial V, template <std::size_t> class F, std::size_t page_size>
requires File<F<page_size>, page_size> std::size_t
PagePool<V, F, page_size>::GetFreeSlot() {
  // TODO: make this more efficient.

  // Look for a free slot.
  for (int i = 0; i < index_to_pages_.size(); i++) {
    if (index_to_pages_[i] == std::nullopt) {
      return i;
    }
  }

  // Next, look for a clean page.
  for (int i = 0; i < dirty_.size(); i++) {
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

template <Trivial V, template <std::size_t> class F, std::size_t page_size>
requires File<F<page_size>, page_size>
void PagePool<V, F, page_size>::EvictSlot(int pos) {
  // Write to file if dirty.
  if (dirty_[pos]) {
    // TODO: update hash before writing it to file.
    file_->StorePage(*index_to_pages_[pos], pool_[pos].AsRawData());
    dirty_[pos] = false;
  }
  pages_to_index_.erase(*index_to_pages_[pos]);
  index_to_pages_[pos] = std::nullopt;
}

}  // namespace carmen::backend::store
