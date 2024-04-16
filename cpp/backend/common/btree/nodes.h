/*
 * Copyright (c) 2024 Fantom Foundation
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE file and at fantom.foundation/bsl11.
 *
 * Change Date: 2028-4-16
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by the GNU Lesser General Public Licence v3.
 */

#pragma once

#include <algorithm>
#include <cstddef>
#include <optional>
#include <span>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "backend/common/btree/entry.h"
#include "backend/common/btree/insert_result.h"
#include "backend/common/page.h"
#include "backend/common/page_id.h"
#include "common/status_util.h"
#include "common/type.h"
#include "common/variant_util.h"

namespace carmen::backend::btree {

// This header file defines nodes types for B-Tree based data structures. They
// are defined in isolation to support individual unit test coverage of them.

// ----------------------------------------------------------------------------
//                               Declarations
// ----------------------------------------------------------------------------

namespace internal {

// A common base type for nodes covering the basic page requirements.
template <typename Derived>
class alignas(kFileSystemPageSize) Node {
 public:
  operator std::span<const std::byte, kFileSystemPageSize>() const {
    return std::span<const std::byte, kFileSystemPageSize>(
        reinterpret_cast<const std::byte*>(this), sizeof(Derived));
  }

  operator std::span<std::byte, kFileSystemPageSize>() {
    return std::span<std::byte, kFileSystemPageSize>(
        reinterpret_cast<std::byte*>(this), sizeof(Derived));
  }
};

}  // namespace internal

// A LeafNode is the node type used in the leaf nodes of BTrees. It contains
// a sorted list of entries, each containing a key and an optional value. Unless
// the leaf node is also the root (for very small trees), there are at least
// kMaxEntries/2 entries in each leaf.
//
// This implementation supports the customization of the key and value type, the
// ordering of entries in the tree (throug the Less) operator, and the
// customization of the maximum number of entries per node. By default, when
// setting `max_entries` to 0, the node will fit as many entries as possible
// in a single node without exceeding the file systems page size.
template <Trivial Key, Trivial Value = Unit, typename Less = std::less<Key>,
          std::size_t max_entries = 0>
class LeafNode final
    : public internal::Node<LeafNode<Key, Value, Less, max_entries>> {
 public:
  using entry_t = Entry<Key, Value>;
  using key_t = Key;
  using value_t = Value;
  using less_t = Less;

  // Constant determining whether this node is used in a set or map.
  constexpr static const bool kIsSet = std::is_same_v<value_t, Unit>;

  // A constant implementing the order of keys in this node.
  static const Less kLess;

  // The maximum number of entries stored in this node.
  constexpr static int kMaxEntries =
      max_entries <= 0 ? ((kFileSystemPageSize - sizeof(std::uint16_t) -
                           2 * sizeof(PageId)) /
                          sizeof(entry_t))
                       : max_entries;

  // Provides the current number of elements in this node.
  std::uint16_t Size() const { return num_entries_; }

  // Tests whether the given key is present in this node.
  bool Contains(const Key& key) const;

  // Finds the position of the entry associated to the given key in this node or
  // returns a value >= Size() if not found.
  std::uint16_t Find(const Key& key) const;

  // Inserts the given entry into this node. Entries are identified by their
  // key. If the same key was already present, no operation is conducted.
  // Possible returns are `EntryPresent`, indicating that no modifiacation was
  // necessary, `EntryAdded` stating that the entry was added in the node,
  // growing it by one entry but not exceeding its capacity, and `Split`
  // signaling that the node had to be split to include the new entry and that
  // the split key should be inserted in the parent.
  template <typename PageManager>
  absl::StatusOr<InsertResult<Key>> Insert(PageId this_page_id,
                                           const entry_t& entry,
                                           PageManager& manager);

  // Obtains a reference to the value at the given position. No bounds are
  // checked.
  const entry_t& At(int pos) const { return entries_[pos]; }

  // Returns the predecessor of this node, 0 if there is none.
  PageId GetPredecessor() const { return prev_; }

  // Returns the successor of this node, 0 if there is none.
  PageId GetSuccessor() const { return next_; }

  // For testing: checks the invariants defined for this node, returns an error
  // if one is violated. The provided bounds should define lower and upper
  // bounds (exclusive) for the values stored in this set. If null, the
  // respective bound is ignored.
  absl::Status Check(const Key* lower_bound, const Key* upper_bound) const;

  // For testing: provides a view on the contained entries.
  std::span<const entry_t> GetEntries() const {
    return {entries_.begin(), entries_.begin() + num_entries_};
  };

  // For testing, to set the entries in this node. Note: this function does not
  // retain the ordered constraint on entries nor does it check boundaries. It
  // is not intended to be used outside of tests.
  void SetTestEntries(std::span<const entry_t> data) {
    for (std::size_t i = 0; i < data.size(); i++) {
      entries_[i] = data[i];
    }
    num_entries_ = data.size();
  }

  // For debugging: prints the tree to std::cout.
  void Print() const;

 private:
  // An internal utility function to insert entries in a given position of the
  // entry array.
  void InsertAt(std::size_t position, const entry_t& entry);

  // Sorted list of num_entries_ values. The value of entries beyond
  // num_entries_ is undefined.
  std::array<entry_t, kMaxEntries> entries_;

  // The number of entries stored in this node.
  std::uint16_t num_entries_;

  // The ID of the preceeding leaf node in the BTree, to aid iteration.
  PageId prev_;

  // The ID of the succeeding leaf node in the BTree, to aid iteration.
  PageId next_;
};

// A InnerNode is the format of a inner node of a BTree set, containing keys
// sub-dividing the range between sub-trees. Each node containing K keys
// references K+1 sub-trees through page numbers. Each inner node that is not
// the root af a tree contains at least kMaxKeys/2 keys.
//
// Inner nodes do only retain keys and child page IDs, yet no information on
// their level within the tree to be memory efficient. This kind of information
// can be maintained while navigating trees.
//
// This implementation derives entry and order types from a leaf node
// definition. The only node-specific customization that can be made is the
// maximum number of keys per node, which may be reduced to construct simpler
// test cases. By default, this node type uses the maximum number of keys
// without exceeding the file-page size limit.
template <Page LeafNode, std::size_t max_keys = 0>
class InnerNode final : public internal::Node<InnerNode<LeafNode, max_keys>> {
 public:
  using leaf_t = LeafNode;
  using entry_t = typename LeafNode::entry_t;
  using key_t = typename LeafNode::key_t;
  using value_t = typename LeafNode::value_t;

  // The maximum number of keys to be stored in this node.
  constexpr static std::size_t kMaxKeys =
      max_keys <= 0
          ? ((kFileSystemPageSize - sizeof(std::uint16_t) - sizeof(PageId)) /
             (sizeof(key_t) + sizeof(PageId)))
          : max_keys;

  // This function initializes an inner node with a given pair of sub-trees and
  // a single key. This function is used to initialize new roots in case the
  // BTree needs to grow.
  void Init(PageId left, const key_t& key, PageId right);

  // Tests whether the given key is present in this node. The provided level
  // is the level of this node within the tree, 0 being the leaf level, 1 being
  // the next level up, and so forth. The provided PageManager is used to
  // resolve pages for recursive calls to child pages.
  template <typename PageManager>
  absl::StatusOr<bool> Contains(std::uint16_t level, const key_t& key,
                                PageManager& manager) const;

  // Finds the location of the position after the last element in this sub-tree.
  template <typename PageManager>
  absl::StatusOr<std::pair<const LeafNode*, std::uint16_t>> End(
      std::uint16_t level, PageManager& manager) const;

  // Finds the page and index possition of the entry associated to the given key
  // in the sub-tree rooted by this node or page 0 if there is no such entry.
  // The parameters are the same as for the Contains(..) above.
  template <typename PageManager>
  absl::StatusOr<std::pair<const LeafNode*, std::uint16_t>> Find(
      std::uint16_t level, const key_t& key, PageManager& manager) const;

  // Inserts the given entry in this node or one of its sub-trees. The level is
  // required to identify the leaf level, and the page manager is used to
  // resolve child nodes as required. The result range may be the same as for
  // leaf nodes: EntryAdded, EntryPresent, or Split.
  template <typename PageManager>
  absl::StatusOr<InsertResult<key_t>> Insert(PageId this_page_id,
                                             std::uint16_t level,
                                             const entry_t& entry,
                                             PageManager& manager);

  // Checks internal invariants of this node and its child nodes. Those
  // invariants include the minimum number of keys and their ordering
  // constraints.
  template <typename PageManager>
  absl::Status Check(std::uint16_t level, const key_t* lower_bound,
                     const key_t* upper_bound, PageManager& manager) const;

  // For testing only: Appends a key/child pair at the end of the key list.
  void Append(const key_t& key, PageId child);

  // For testing only: retrieves a view on the contained keys.
  std::span<const key_t> GetKeys() const {
    return {keys_.begin(), keys_.begin() + num_keys_};
  };

  // For testing only: retrieves a view on the contained child page IDs.
  std::span<const PageId> GetChildren() const {
    return {children_.begin(), children_.begin() + num_keys_ + 1};
  };

  // For debugging: prints the tree to std::cout.
  template <typename PageManager>
  void Print(std::uint16_t level, std::uint16_t indent,
             PageManager& manager) const;

 private:
  // Inserts the given key/child pair at the provided position of the key list.
  void InsertAt(std::size_t position, const key_t& key, PageId child);

  // Sorted list of keys, where only the first num_keys_ are valid.
  std::array<key_t, kMaxKeys> keys_;

  // Child pointers to sub-trees. A pointer at position i references the
  // subtree containing all elements between keys_[i-1] and keys_[i]. The
  // first subtree contains elements less than the first key, while the last
  // subtree contains elements bigger or equal than the last key.
  std::array<PageId, kMaxKeys + 1> children_;

  // The number of keys stored in this node. This is >= kMaxKeys/2 for all
  // nodes but the root node. For the root node this is >= 1.
  std::uint16_t num_keys_;
};

// ----------------------------------------------------------------------------
//                               Definitions
// ----------------------------------------------------------------------------

// ------------------------------- LeafNode -----------------------------------

template <Trivial Key, Trivial Value, typename Less, std::size_t max_entries>
const Less LeafNode<Key, Value, Less, max_entries>::kLess;

template <Trivial Key, Trivial Value, typename Less, std::size_t max_entries>
bool LeafNode<Key, Value, Less, max_entries>::Contains(const key_t& key) const {
  // The elements are sorted, so we can use binary search.
  auto begin = entries_.begin();
  auto end = begin + num_entries_;
  return std::binary_search(
      begin, end, entry_t{key},
      [](const entry_t& a, const entry_t& b) { return kLess(a.key, b.key); });
}

template <Trivial Key, Trivial Value, typename Less, std::size_t max_entries>
std::uint16_t LeafNode<Key, Value, Less, max_entries>::Find(
    const key_t& key) const {
  auto begin = entries_.begin();
  auto end = begin + num_entries_;
  auto pos = std::lower_bound(
      begin, end, entry_t{key},
      [](const entry_t& a, const entry_t& b) { return kLess(a.key, b.key); });
  if (pos == end || pos->key != key) {
    return num_entries_;
  }
  return pos - begin;
}

template <Trivial Key, Trivial Value, typename Less, std::size_t max_entries>
template <typename PageManager>
absl::StatusOr<InsertResult<Key>>
LeafNode<Key, Value, Less, max_entries>::Insert(PageId this_page_id,
                                                const entry_t& entry,
                                                PageManager& context) {
  // Elements are inserted in-order.
  // The first step is to find the insertion position.
  auto begin = entries_.begin();
  auto end = begin + num_entries_;
  auto pos = std::lower_bound(
      begin, end, entry,
      [](const entry_t& a, const entry_t& b) { return kLess(a.key, b.key); });

  // If the key is already present, we are done.
  if (pos < end && pos->key == entry.key) return EntryPresent{};

  // At this point it is clear that the node needs to be modified.
  context.MarkAsDirty(this_page_id);

  // If there is enough space, we can add it to the current node.
  if (num_entries_ < kMaxEntries) {
    InsertAt(pos - begin, entry);
    return EntryAdded{};
  }

  // If this leaf is full, it needs to be split. So we need a new node.
  ASSIGN_OR_RETURN((auto [new_page_id, new_page]),
                   context.template New<LeafNode>());
  context.MarkAsDirty(new_page_id);
  auto& left = *this;
  auto& right = new_page;

  // Fix linking between the leafs.
  right.next_ = left.next_;
  right.prev_ = this_page_id;
  left.next_ = new_page_id;

  if (right.next_ != 0) {
    ASSIGN_OR_RETURN(LeafNode & next,
                     context.template Get<LeafNode>(right.next_));
    context.MarkAsDirty(right.next_);
    next.prev_ = new_page_id;
  }

  // Partition entries in retained elements, the new dividing key (=split_key),
  // and the elements to be moved to the new node.
  const auto mid_index = kMaxEntries / 2;
  auto split_index = mid_index;

  // If the new element ends up in the left node, make the left node one element
  // smaller to be balanced in the end.
  if (pos - begin <= mid_index) {
    split_index--;
  }

  left.num_entries_ = split_index + 1;
  right.num_entries_ = kMaxEntries - split_index - 1;
  std::memcpy(right.entries_.begin(), begin + split_index + 1,
              sizeof(entry_t) * right.num_entries_);

  // Finally, we need to decide where to put the new value.
  if (pos - begin <= mid_index) {
    // The new value ends up in the left node.
    left.InsertAt(pos - begin, entry);
  } else {
    // The new value ends up in the right node.
    right.InsertAt(pos - begin - split_index - 1, entry);
  }

  // In case of a map, the key to be propagated to the parent is the lower bound
  // of the new right node.
  return Split<Key>{right.entries_[0].key, new_page_id};
}

template <Trivial Key, Trivial Value, typename Less, std::size_t max_entries>
void LeafNode<Key, Value, Less, max_entries>::InsertAt(std::size_t position,
                                                       const entry_t& entry) {
  assert(num_entries_ < kMaxEntries);
  assert(0 <= position);
  assert(position <= num_entries_);
  auto pos = entries_.begin() + position;
  auto end = entries_.begin() + num_entries_;
  std::memmove(pos + 1, pos, sizeof(entry_t) * (end - pos));
  *pos = entry;
  num_entries_++;
}

template <Trivial Key, Trivial Value, typename Less, std::size_t max_entries>
absl::Status LeafNode<Key, Value, Less, max_entries>::Check(
    const Key* lower_bound, const Key* upper_bound) const {
  // The node is the root if there is no upper or lower boundary.
  bool is_root = lower_bound == nullptr && upper_bound == nullptr;
  if (!is_root) {
    // Any non-root node must be at least half full.
    if (num_entries_ < kMaxEntries / 2) {
      return absl::InternalError(absl::StrFormat(
          "Invalid number of entries, expected at least %d, got %d",
          kMaxEntries / 2, num_entries_));
    }
  }

  if (num_entries_ == 0) return absl::OkStatus();

  // Check that entries are ordered.
  for (unsigned i = 0; i < num_entries_ - 1; i++) {
    if (!kLess(entries_[i].key, entries_[i + 1].key)) {
      return absl::InternalError("Invalid order of entries");
    }
  }

  // Check bounds
  if (lower_bound != nullptr && !kLess(*lower_bound, entries_[0].key) &&
      entries_[0].key != *lower_bound) {
    return absl::InternalError(
        "Lower boundary is not less-equal than smallest entry");
  }
  if (upper_bound != nullptr &&
      !kLess(entries_[num_entries_ - 1].key, *upper_bound)) {
    return absl::InternalError("Biggest entry is not less than upper boundary");
  }

  return absl::OkStatus();
}

template <Trivial Key, Trivial Value, typename Less, std::size_t max_entries>
void LeafNode<Key, Value, Less, max_entries>::Print() const {
  std::cout << "[";
  for (std::size_t i = 0; i < num_entries_; i++) {
    if (i > 0) {
      std::cout << ", ";
    }
    std::cout << entries_[i];
  }
  std::cout << "] // size=" << num_entries_ << "/" << kMaxEntries << "\n";
}

// ----------------------------- InnerNode ------------------------------------

template <Page LeafNode, std::size_t max_keys>
template <typename PageManager>
absl::StatusOr<bool> InnerNode<LeafNode, max_keys>::Contains(
    std::uint16_t level, const key_t& key, PageManager& manager) const {
  // Search lower bound for the key range to identify next node.
  auto begin = keys_.begin();
  auto end = begin + num_keys_;
  auto pos = std::lower_bound(begin, end, key, LeafNode::kLess);
  if (pos != end && *pos == key) {
    return true;
  }
  PageId next = children_[pos - begin];
  if (level > 1) {
    ASSIGN_OR_RETURN(InnerNode & node, manager.template Get<InnerNode>(next));
    return node.Contains(level - 1, key, manager);
  } else {
    ASSIGN_OR_RETURN(LeafNode & node, manager.template Get<LeafNode>(next));
    return node.Contains(key);
  }
}

template <Page LeafNode, std::size_t max_keys>
template <typename PageManager>
absl::StatusOr<std::pair<const LeafNode*, std::uint16_t>>
InnerNode<LeafNode, max_keys>::End(std::uint16_t level,
                                   PageManager& manager) const {
  // The end is found in the right-most sub-tree.
  PageId next = children_[num_keys_];
  if (level > 1) {
    ASSIGN_OR_RETURN(InnerNode & node, manager.template Get<InnerNode>(next));
    return node.End(level - 1, manager);
  } else {
    ASSIGN_OR_RETURN(LeafNode & node, manager.template Get<LeafNode>(next));
    return std::make_pair(&node, node.Size());
  }
}

template <Page LeafNode, std::size_t max_keys>
template <typename PageManager>
absl::StatusOr<std::pair<const LeafNode*, std::uint16_t>>
InnerNode<LeafNode, max_keys>::Find(std::uint16_t level, const key_t& key,
                                    PageManager& manager) const {
  // Search upper bound for the key range to identify next node.
  auto begin = keys_.begin();
  auto end = begin + num_keys_;
  auto pos = std::upper_bound(begin, end, key, LeafNode::kLess);
  PageId next = children_[pos - begin];
  if (level > 1) {
    ASSIGN_OR_RETURN(InnerNode & node, manager.template Get<InnerNode>(next));
    return node.Find(level - 1, key, manager);
  } else {
    ASSIGN_OR_RETURN(LeafNode & node, manager.template Get<LeafNode>(next));
    auto pos = node.Find(key);
    if (pos >= node.Size()) {
      return std::make_pair(nullptr, 0);
    }
    return std::make_pair(&node, pos);
  }
}

template <Page LeafNode, std::size_t max_keys>
template <typename PageManager>
absl::StatusOr<InsertResult<typename LeafNode::key_t>>
InnerNode<LeafNode, max_keys>::Insert(PageId this_page_id, std::uint16_t level,
                                      const entry_t& entry,
                                      PageManager& manager) {
  auto begin = keys_.begin();
  auto end = begin + num_keys_;
  auto pos = std::lower_bound(begin, end, entry.key, LeafNode::kLess);
  if (pos < end && *pos == entry.key) {
    return EntryPresent{};
  }
  auto next = children_[pos - begin];

  InsertResult<key_t> result;
  if (level > 1) {
    // Next level is a inner node, insert there.
    ASSIGN_OR_RETURN(InnerNode & node, (manager.template Get<InnerNode>(next)));
    ASSIGN_OR_RETURN(result, node.Insert(next, level - 1, entry, manager));
  } else {
    // Next level is a leaf node, insert there.
    ASSIGN_OR_RETURN(LeafNode & node, (manager.template Get<LeafNode>(next)));
    ASSIGN_OR_RETURN(result, node.Insert(next, entry, manager));
  }

  // At this point *this page may have been replaced in the page pool!
  // => currently, we assume the page pool is large enough and the pool policy
  // keeps the pages present long enough; in the future, page pinning needs to
  // be implemented;
  // TODO: pin this page before doing the recursive call above.

  return std::visit(
      match{[&](const Split<key_t>& split)
                -> absl::StatusOr<InsertResult<key_t>> {
              // We need to modify this node, so in any case it is dirty after.
              manager.MarkAsDirty(this_page_id);

              // The child node was split and we need to integrate a new key
              // in this inner node. If there is still enough capacity, we can
              // just add it in the current node.
              if (num_keys_ < kMaxKeys) {
                InsertAt(pos - begin, split.key, split.new_tree);
                return EntryAdded{};
              }

              // If this node is full, it needs to be split. So we need a new
              // node.
              ASSIGN_OR_RETURN((auto [new_page_id, new_page]),
                               manager.template New<InnerNode>());
              manager.MarkAsDirty(new_page_id);
              auto& left = *this;
              auto& right = new_page;

              // Partition entries in retained elements, the new dividing key
              // (=split_key), and the elements to be moved to the new node.
              const std::ptrdiff_t mid_index = kMaxKeys / 2 + (kMaxKeys % 2);
              auto split_index = mid_index;

              // If the new value would end up right at the split position, use
              // the new value as the split key and devide elements evenly
              // left/right.
              if (pos - begin == split_index) {
                left.num_keys_ = split_index;
                right.num_keys_ = kMaxKeys - split_index;
                std::memcpy(right.keys_.begin(), begin + split_index,
                            sizeof(key_t) * right.num_keys_);
                right.children_[0] = split.new_tree;
                std::memcpy(right.children_.begin() + 1,
                            left.children_.begin() + split_index + 1,
                            sizeof(PageId) * right.num_keys_);
                return Split<key_t>{split.key, new_page_id};
              }

              // If the new element ends up in the left node, make the left node
              // one element smaller to be balanced in the end.
              if (pos - begin < split_index) {
                split_index--;
              }

              auto split_key = left.keys_[split_index];
              left.num_keys_ = split_index;
              right.num_keys_ = kMaxKeys - split_index - 1;
              std::memcpy(right.keys_.begin(), begin + split_index + 1,
                          sizeof(key_t) * (right.num_keys_));
              std::memcpy(right.children_.begin(),
                          left.children_.begin() + split_index + 1,
                          sizeof(PageId) * (right.num_keys_ + 1));

              // Finally, we need to decide where to put the new value.
              if (pos - begin < mid_index) {
                // The new value ends up in the left node.
                left.InsertAt(pos - begin, split.key, split.new_tree);
              } else {
                // The new value ends up in the right node.
                right.InsertAt(pos - begin - split_index - 1, split.key,
                               split.new_tree);
              }

              return Split<key_t>{split_key, new_page_id};
            },
            // If not split, the insert result only needs to be propagated up.
            [](EntryAdded added) -> absl::StatusOr<InsertResult<key_t>> {
              return added;
            },
            [](EntryPresent present) -> absl::StatusOr<InsertResult<key_t>> {
              return present;
            }},
      result);
}

template <Page LeafNode, std::size_t max_keys>
void InnerNode<LeafNode, max_keys>::Init(PageId left,
                                         const typename LeafNode::key_t& key,
                                         PageId right) {
  num_keys_ = 1;
  keys_[0] = key;
  children_[0] = left;
  children_[1] = right;
}

template <Page LeafNode, std::size_t max_keys>
void InnerNode<LeafNode, max_keys>::Append(const key_t& key, PageId child) {
  assert(num_keys_ < kMaxKeys);
  keys_[num_keys_] = key;
  children_[num_keys_ + 1] = child;
  num_keys_++;
}

template <Page LeafNode, std::size_t max_keys>
template <typename PageManager>
absl::Status InnerNode<LeafNode, max_keys>::Check(std::uint16_t level,
                                                  const key_t* lower_bound,
                                                  const key_t* upper_bound,
                                                  PageManager& manager) const {
  // The node is the root if there is no upper or lower boundary.
  bool is_root = lower_bound == nullptr && upper_bound == nullptr;
  if (!is_root) {
    // Any non-root node must be at least half full.
    if (num_keys_ < kMaxKeys / 2) {
      return absl::InternalError(absl::StrFormat(
          "Invalid number of keys, expected at least %d, got %d", kMaxKeys / 2,
          num_keys_));
    }
  } else {
    // The root node must have at least 1 key.
    if (num_keys_ < 1) {
      return absl::InternalError("Root node must have at least one key");
    }
  }

  // Check that keys are ordered.
  for (unsigned i = 0; i < num_keys_ - 1; i++) {
    if (!LeafNode::kLess(keys_[i], keys_[i + 1])) {
      return absl::InternalError("Invalid order of keys");
    }
  }

  // Check bounds
  if (lower_bound != nullptr && !LeafNode::kLess(*lower_bound, keys_[0])) {
    return absl::InternalError("Lower boundary is not less than smallest key");
  }
  if (upper_bound != nullptr &&
      !LeafNode::kLess(keys_[num_keys_ - 1], *upper_bound)) {
    return absl::InternalError("Biggest key is not less than upper boundary");
  }

  // Check child nodes
  auto check_child = [&](PageId id, const key_t* lower,
                         const key_t* upper) -> absl::Status {
    if (level > 1) {
      ASSIGN_OR_RETURN(InnerNode & child, manager.template Get<InnerNode>(id));
      return child.Check(level - 1, lower, upper, manager);
    } else {
      ASSIGN_OR_RETURN(LeafNode & child, manager.template Get<LeafNode>(id));
      return child.Check(lower, upper);
    }
  };

  RETURN_IF_ERROR(check_child(children_[0], lower_bound, &keys_[0]));
  for (unsigned i = 0; i < num_keys_ - 1; i++) {
    RETURN_IF_ERROR(check_child(children_[i + 1], &keys_[i], &keys_[i + 1]));
  }
  RETURN_IF_ERROR(
      check_child(children_[num_keys_], &keys_[num_keys_ - 1], upper_bound));

  return absl::OkStatus();
}

template <Page LeafNode, std::size_t max_keys>
template <typename PageManager>
void InnerNode<LeafNode, max_keys>::Print(std::uint16_t level,
                                          std::uint16_t indent,
                                          PageManager& manager) const {
  std::string indent_str(indent * 4 + 4, ' ');
  std::cout << indent_str << "Node: size=" << num_keys_ << "/" << kMaxKeys
            << "\n";

  if (num_keys_ < 1) {
    std::cout << indent_str << "   - invalid empty inner node --\n";
  }

  auto print_child = [&](PageId page) {
    if (level > 1) {
      auto node = manager.template Get<InnerNode>(page);
      if (!node.ok()) {
        std::cout << indent_str
                  << "    - failed to load page: " << node.status() << "\n";
      } else {
        node->get().Print(level - 1, indent + 1, manager);
      }
    } else {
      auto node = manager.template Get<LeafNode>(page);
      std::cout << indent_str << "    ";
      if (!node.ok()) {
        std::cout << "- failed to load page: " << node.status() << "\n";
      } else {
        node->get().Print();
      }
    }
  };

  for (unsigned i = 0; i < num_keys_; i++) {
    print_child(children_[i]);
    std::cout << indent_str << keys_[i] << ":\n";
  }
  print_child(children_[num_keys_]);
}

template <Page LeafNode, std::size_t max_keys>
void InnerNode<LeafNode, max_keys>::InsertAt(std::size_t position,
                                             const key_t& key, PageId child) {
  assert(0 <= position);
  assert(position <= num_keys_);
  assert(num_keys_ < kMaxKeys);

  // Insert key at requested position.
  {
    auto begin = keys_.begin();
    auto pos = begin + position;
    auto end = begin + num_keys_;
    std::memmove(pos + 1, pos, sizeof(key) * (end - pos));
    *pos = key;
  }

  // Insert new child pointer at the key's position + 1.
  {
    auto begin = children_.begin();
    auto pos = begin + position + 1;
    auto end = begin + num_keys_ + 1;
    std::memmove(pos + 1, pos, sizeof(PageId) * (end - pos));
    *pos = child;
  }

  num_keys_++;
}

}  // namespace carmen::backend::btree
