#pragma once

#include "absl/container/btree_set.h"
#include "common/type.h"

namespace carmen::backend::multimap {

template <Trivial K, Trivial V>
class InMemoryMultiMap {
 public:
  bool Insert(const K& key, const V& value) {
    return set_.insert({key, value}).second;
  }

  bool Contains(const K& key, const V& value) const {
    return set_.contains({key, value});
  }

  void Erase(const K& key) {
    auto [from, to] = GetKeyRange(key);
    set_.erase(from, to);
  }

  void Erase(const K& key, const V& value) { set_.erase({key, value}); }

  template <typename Op>
  void ForEach(const K& key, const Op& op) const {
    auto [from, to] = GetKeyRange(key);
    for (auto it = from; it != to; ++it) {
      op(it->second);
    }
  }

  // For testing only.
  template <typename Op>
  void ForEach(const Op& op) const {
    for (auto it = set_.begin(); it != set_.end(); ++it) {
      op(it->first, it->second);
    }
  }

 private:
  using entry = std::pair<K, V>;

  auto GetKeyRange(const K& key) const {
    return std::make_pair(set_.lower_bound({key, 0}),
                          set_.lower_bound({key + 1, 0}));
  }

  absl::btree_set<entry> set_;
};

}  // namespace carmen::backend::multimap
