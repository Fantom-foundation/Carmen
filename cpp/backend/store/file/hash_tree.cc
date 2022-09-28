#include "backend/store/file/hash_tree.h"

#include <cstddef>
#include <span>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "common/hash.h"
#include "common/type.h"

namespace carmen::backend::store {

void HashTree::UpdateHash(PageId id, std::span<const std::byte> page) {
  UpdateHash(id, carmen::GetHash(hasher_, page));
}

void HashTree::UpdateHash(PageId id, const Hash& hash) {
  TrackNumPages(id);
  GetHash(0, id) = hash;
  dirty_pages_.erase(id);
  dirty_level_one_positions_.insert(id / branching_factor_);
}

void HashTree::MarkDirty(PageId page) {
  TrackNumPages(page);
  dirty_pages_.insert(page);
}

Hash HashTree::GetHash() {
  // If there are no pages, the full hash is zero by definition.
  if (num_pages_ == 0) {
    return Hash{};
  }

  // If nothing has changed in the meanwhile, return the last result.
  if (dirty_pages_.empty() && dirty_level_one_positions_.empty()) {
    return hashes_.back()[0];
  }

  // Update hashes of dirty pages.
  absl::flat_hash_set<int> dirty_parent;
  for (PageId i : dirty_pages_) {
    auto data = page_source_->GetPageData(i);
    GetHash(0, i) = carmen::GetHash(hasher_, data);
    dirty_parent.insert(i / branching_factor_);
  }
  dirty_pages_.clear();

  // If there is only one page, the full hash is that page's hash.
  if (num_pages_ == 1) {
    return GetHash(0, 0);
  }

  // Complete list of level-1 nodes that are dirty.
  dirty_parent.insert(dirty_level_one_positions_.begin(),
                      dirty_level_one_positions_.end());
  dirty_level_one_positions_.clear();

  // Perform hash aggregation.
  for (int level = 1;; level++) {
    // Gets the parent before the children because the fetching of the parent
    // may resize the hash list while the fetch for the children will not.
    std::vector<Hash>& parent = GetHashes(level);
    const std::vector<Hash>& children = GetHashes(level - 1);

    absl::flat_hash_set<int> new_dirty;
    for (int parent_pos : dirty_parent) {
      auto data = std::as_bytes(std::span<const Hash>(children).subspan(
          parent_pos * branching_factor_, branching_factor_));
      GetHash(level, parent_pos) = carmen::GetHash(hasher_, data);
      new_dirty.insert(parent_pos / branching_factor_);
    }

    if (children.size() <= branching_factor_) {
      return parent[0];
    }

    std::swap(dirty_parent, new_dirty);
  }
}

namespace {

std::size_t GetPaddedSize(std::size_t min_size, std::size_t block_size) {
  if (min_size % block_size == 0) {
    return min_size;
  }
  return ((min_size / block_size) + 1) * block_size;
}

}  // namespace

std::vector<Hash>& HashTree::GetHashes(int level) {
  if (level >= hashes_.size()) {
    hashes_.resize(level + 1);
  }
  return hashes_[level];
}

Hash& HashTree::GetHash(int level, int pos) {
  auto& level_hashes = GetHashes(level);
  if (pos >= level_hashes.size()) {
    level_hashes.resize(GetPaddedSize(pos + 1, branching_factor_));
  }
  return level_hashes[pos];
}

void HashTree::TrackNumPages(PageId page) {
  if (page < num_pages_) {
    return;
  }

  // All new pages need to be considered dirty.
  for (auto cur = num_pages_; cur <= page; cur++) {
    dirty_pages_.insert(cur);
  }
  num_pages_ = page+1;
}

}  // namespace carmen::backend::store
