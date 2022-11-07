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
    : entries_(size) {
  auto last = &(entries_[0]);
  for (auto& cur : entries_) {
    last->succ = &cur;
    last = &cur;
  }
  last->succ = nullptr;
}

void LeastRecentlyUsedEvictionPolicy::Read(std::size_t position) {
  // Position must be in range.
  assert(position >= 0 && position < entries_.size());
  Entry* cur = &entries_[position];
  if (head_ == cur) {
    return;
  }

  // Remove element from current position in the list.
  if (cur->pred) {
    cur->pred->succ = cur->succ;
  }
  if (cur->succ) {
    cur->succ->pred = cur->pred;
  } else if (tail_ == cur) {
    tail_ = cur->pred;
  }

  // Add current element at top of list.
  cur->pred = nullptr;
  cur->succ = head_;
  if (head_) {
    head_->pred = cur;
  }
  head_ = cur;
  if (tail_ == nullptr) {
    tail_ = cur;
  }
}

void LeastRecentlyUsedEvictionPolicy::Written(std::size_t position) {
  // This policy does not distinguish between read an writes.
  Read(position);
}

void LeastRecentlyUsedEvictionPolicy::Removed(std::size_t position) {
  // Position must be in range.
  assert(position >= 0 && position < entries_.size());
  Entry* cur = &entries_[position];
  auto pred = cur->pred;
  auto succ = cur->succ;
  if (pred) {
    cur->pred->succ = succ;
    cur->pred = nullptr;
  }
  if (succ) {
    cur->succ->pred = pred;
    cur->succ = nullptr;
  }
  if (head_ == cur) {
    head_ = succ;
  }
  if (tail_ == cur) {
    tail_ = pred;
  }
}

std::optional<std::size_t> LeastRecentlyUsedEvictionPolicy::GetPageToEvict() {
  if (tail_ == nullptr) {
    return std::nullopt;
  }
  return tail_ - &entries_[0];
}

void LeastRecentlyUsedEvictionPolicy::Dump() {
  std::cout << "List:\n";
  Entry* first = &entries_[0];
  Entry* cur = head_;
  while (cur != nullptr) {
    std::cout << cur - first << " ";
    cur = cur->succ;
  }
  std::cout << "\n";

  cur = tail_;
  while (cur != nullptr) {
    std::cout << cur - first << " ";
    cur = cur->pred;
  }
  std::cout << "\n\n";
}

}  // namespace carmen::backend
