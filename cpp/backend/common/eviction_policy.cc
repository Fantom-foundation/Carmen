#include "backend/common/eviction_policy.h"

#include <cstddef>
#include <cstdlib>
#include <optional>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"

namespace carmen::backend {

namespace {

std::size_t PickRandom(const absl::flat_hash_set<std::size_t> values) {
  auto pos = rand() % values.size();
  auto iter = values.begin();
  for (std::size_t i = 0; i < pos; i++) {
    iter++;
  }
  return *iter;
}

}  // namespace

RandomEvictionPolicy::RandomEvictionPolicy(std::size_t size)
    : clean_(size), dirty_(size) {}

void RandomEvictionPolicy::Read(std::size_t position) {
  if (!dirty_.contains(position)) {
    clean_.insert(position);
  }
}

void RandomEvictionPolicy::Written(std::size_t position) {
  clean_.erase(position);
  dirty_.insert(position);
}

void RandomEvictionPolicy::Removed(std::size_t position) {
  dirty_.erase(position);
  clean_.erase(position);
}

std::optional<std::size_t> RandomEvictionPolicy::GetPageToEvict() {
  if (!clean_.empty()) {
    return PickRandom(clean_);
  }
  if (!dirty_.empty()) {
    return PickRandom(dirty_);
  }
  return std::nullopt;
}

LeastRecentlyUsedEvictionPolicy::LeastRecentlyUsedEvictionPolicy(
    std::size_t size)
    : index_(size) {}

void LeastRecentlyUsedEvictionPolicy::Read(std::size_t position) {
  auto [pos, new_entry] = index_.insert({position, nullptr});
  Entry* cur;
  if (new_entry) {
    if (free_ != nullptr) {
      cur = free_;
      free_ = free_->succ;
    } else {
      entries_.push_back({});
      cur = &entries_.back();
    }
    pos->second = cur;
    cur->position = position;
    if (tail_ == nullptr) {
      tail_ = cur;
    }
  } else {
    cur = pos->second;
    if (head_ == cur) {
      return;
    }

    // Remove element from current position in the list.
    cur->pred->succ = cur->succ;
    if (cur->succ) {
      cur->succ->pred = cur->pred;
    } else {
      tail_ = cur->pred;
    }
  }
  // Add current element at top of list.
  cur->pred = nullptr;
  cur->succ = head_;
  if (head_) {
    head_->pred = cur;
  }
  head_ = cur;
}

void LeastRecentlyUsedEvictionPolicy::Written(std::size_t position) {
  // This policy does not distinguish between read an writes.
  Read(position);
}

void LeastRecentlyUsedEvictionPolicy::Removed(std::size_t position) {
  auto pos = index_.find(position);
  if (pos == index_.end()) {
    return;
  }
  Entry* cur = pos->second;
  if (cur->pred) {
    cur->pred->succ = cur->succ;
  } else {
    head_ = cur->succ;
  }

  if (cur->succ) {
    cur->succ->pred = cur->pred;
  } else {
    tail_ = cur->pred;
  }

  index_.erase(pos);

  // Insert current element into free list.
  cur->pred = free_;
  free_ = cur;
}

std::optional<std::size_t> LeastRecentlyUsedEvictionPolicy::GetPageToEvict() {
  if (tail_ == nullptr) {
    return std::nullopt;
  }
  return tail_->position;
}

void LeastRecentlyUsedEvictionPolicy::Dump() {
  std::cout << "List:\n";
  Entry* cur = head_;
  while (cur != nullptr) {
    std::cout << cur->position << " ";
    cur = cur->succ;
  }
  std::cout << "\n";

  cur = tail_;
  while (cur != nullptr) {
    std::cout << cur->position << " ";
    cur = cur->pred;
  }
  std::cout << "\n\n";
}

}  // namespace carmen::backend
