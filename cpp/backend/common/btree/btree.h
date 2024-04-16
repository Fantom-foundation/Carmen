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

#include <filesystem>
#include <optional>

#include "absl/status/statusor.h"
#include "backend/common/btree/entry.h"
#include "backend/common/btree/nodes.h"
#include "backend/common/page_manager.h"
#include "common/type.h"
#include "common/variant_util.h"

namespace carmen::backend::btree {

// ----------------------------------------------------------------------------
//                            BTree Declaration
// ----------------------------------------------------------------------------

// A BTree is an ordered set of entries stored on secondary storage. Each node
// of the tree is a page of a file. Inner nodes contain list of keys and
// child-page pointers, while leaf nodes contain sorted list of entries. Entries
// comprise a key and a value, although the value is ignored if its the Unit
// type.
//
// This BTree implementation is intended to be the common base for the BTreeSet
// and BTreeMap implementation customizing its parameters for the respective use
// case. It is not intended to be used directly.
//
// This implementation can be customized by the types of Keys and Values to be
// stored, the page pool implementation to be used for accessing data, and the
// order in which keys are stored. Also, to ease the testing of deeper trees,
// the default width of inner nodes and leafs can be overridden.
template <Trivial Key, Trivial Value, typename PagePool,
          typename Comparator = std::less<Key>,
          std::size_t max_keys = 0,      // 0 means as many as fit in a page
          std::size_t max_elements = 0>  // 0 means as many as fit in a page
class BTree {
 public:
  // Opens the tree stored in the given directory. If no data is found, an empty
  // tree linked to this directory is created.
  template <typename Derived>
  static absl::StatusOr<Derived> Open(std::filesystem::path directory);

  // Get the number of entries in this tree.
  std::size_t Size() const;

  // Tests whether the given key is contained in this tree.
  absl::StatusOr<bool> Contains(const Key& key) const;

  // Flushes all pending changes to disk.
  absl::Status Flush();

  // Closes this tree by flushing its content and closing the underlying file.
  // After this, no more operations on the tree will be successful.
  absl::Status Close();

  // For testing: checks internal invariants of this data structure.
  absl::Status Check() const;

  // For debugging: Prints the content of this tree to std::cout.
  void Print() const;

 protected:
  using LeafNode = btree::LeafNode<Key, Value, Comparator, max_keys>;
  using InnerNode = btree::InnerNode<LeafNode, max_elements>;

  // An iterator on BTree content. It is modeled after iterators in the standard
  // library, however increment/decrement operators may fail due to IO errors.
  class Iterator {
   public:
    using element_type = Entry<Key, Value>;

    Iterator() = default;

    bool operator==(const Iterator&) const = default;
    const Entry<Key, Value>& operator*() const;
    const Entry<Key, Value>* get() const;

    absl::Status Next();
    absl::Status Previous();

   private:
    friend class BTree;
    Iterator(const PageManager<PagePool>* manager, const LeafNode* node,
             std::uint16_t pos)
        : manager_(manager), node_(node), pos_(pos) {}

    // TODO: use page pinning once implemented
    const PageManager<PagePool>* manager_ = nullptr;
    const LeafNode* node_ = nullptr;
    std::uint16_t pos_ = 0;
  };

  // Returns an iterator pointing to the first element in the tree. If the tree
  // is empty, it is equivalent to the end.
  absl::StatusOr<Iterator> Begin() const;

  // Returns an iterator pointing to the non-existing element after the last
  // element in the tree.
  absl::StatusOr<Iterator> End() const;

  // Returns an iterator pointing to the given key or End() if there is
  // no such key. To mearly check whether an element is present it is more
  // efficient to use the Contains(..) function, since contains may stop in the
  // unlikely case of the key being present in an inner node, while find needs
  // to go all the way to the leaf node to get the value.
  absl::StatusOr<Iterator> Find(const Key& key) const;

  // Inserts the given entry. This function is intended to be used by derived
  // implementations, customized for their use case.
  absl::StatusOr<bool> Insert(const Entry<Key, Value>& entry);

 private:
  // A special page type used to store tree meta data. This node type is always
  // at page 0 of a file.
  struct MetaData : public btree::internal::Node<MetaData> {
    PageId root;
    std::uint64_t num_entries;
    std::uint32_t height;
    // TODO: include page manager state!
  };

  BTree(const MetaData& data, PageManager<PagePool> page_manager);

  // The page ID of the root node.
  PageId root_id_;

  // The total number of entries stored in this tree.
  std::uint64_t num_entries_;

  // The node height of this tree. This is the maximum number of nodes that need
  // to be accessed when navigating from the root to the leaf nodes.
  std::uint32_t height_;

  // The page manager handling the allocation of nodes (=pages).
  PageManager<PagePool> page_manager_;
};

// ----------------------------------------------------------------------------
//                             BTree Definitions
// ----------------------------------------------------------------------------

template <Trivial Key, Trivial Value, typename PagePool, typename Comparator,
          std::size_t max_keys, std::size_t max_elements>
template <typename Derived>
absl::StatusOr<Derived> BTree<Key, Value, PagePool, Comparator, max_keys,
                              max_elements>::Open(std::filesystem::path path) {
  ASSIGN_OR_RETURN(auto file, PagePool::File::Open(path));
  MetaData meta;
  auto num_pages = file.GetNumPages();
  if (num_pages == 0) {
    meta.root = 1;
    meta.num_entries = 0;
    meta.height = 0;
    num_pages++;  // Page 0 is implicitly used for meta data.
  } else {
    RETURN_IF_ERROR(file.LoadPage(0, meta));
  }
  PagePool pool(std::make_unique<typename PagePool::File>(std::move(file)));
  return Derived(meta, PageManager(std::move(pool), num_pages + 1));
}

template <Trivial Key, Trivial Value, typename PagePool, typename Comparator,
          std::size_t max_keys, std::size_t max_elements>
BTree<Key, Value, PagePool, Comparator, max_keys, max_elements>::BTree(
    const MetaData& meta, PageManager<PagePool> page_manager)
    : root_id_(meta.root),
      num_entries_(meta.num_entries),
      height_(meta.height),
      page_manager_(std::move(page_manager)) {}

template <Trivial Key, Trivial Value, typename PagePool, typename Comparator,
          std::size_t max_keys, std::size_t max_elements>
std::size_t
BTree<Key, Value, PagePool, Comparator, max_keys, max_elements>::Size() const {
  return num_entries_;
}

template <Trivial Key, Trivial Value, typename PagePool, typename Comparator,
          std::size_t max_keys, std::size_t max_elements>
absl::StatusOr<bool> BTree<Key, Value, PagePool, Comparator, max_keys,
                           max_elements>::Contains(const Key& key) const {
  if (height_ > 0) {
    ASSIGN_OR_RETURN(InnerNode & inner,
                     page_manager_.template Get<InnerNode>(root_id_));
    return inner.Contains(height_, key, page_manager_);
  }
  ASSIGN_OR_RETURN(LeafNode & leaf,
                   page_manager_.template Get<LeafNode>(root_id_));
  return leaf.Contains(key);
}

template <Trivial Key, Trivial Value, typename PagePool, typename Comparator,
          std::size_t max_keys, std::size_t max_elements>
absl::StatusOr<typename BTree<Key, Value, PagePool, Comparator, max_keys,
                              max_elements>::Iterator>
BTree<Key, Value, PagePool, Comparator, max_keys, max_elements>::Begin() const {
  if (num_entries_ == 0) return End();
  // The first leaf node is always at page 1 since it is created there and never
  // moved.
  ASSIGN_OR_RETURN(LeafNode & leaf, page_manager_.template Get<LeafNode>(1));
  return Iterator(&page_manager_, &leaf, 0);
}

template <Trivial Key, Trivial Value, typename PagePool, typename Comparator,
          std::size_t max_keys, std::size_t max_elements>
absl::StatusOr<typename BTree<Key, Value, PagePool, Comparator, max_keys,
                              max_elements>::Iterator>
BTree<Key, Value, PagePool, Comparator, max_keys, max_elements>::End() const {
  if (height_ > 0) {
    ASSIGN_OR_RETURN(InnerNode & inner,
                     page_manager_.template Get<InnerNode>(root_id_));
    ASSIGN_OR_RETURN((auto [node, pos]), inner.End(height_, page_manager_));
    return Iterator(&page_manager_, node, pos);
  }
  ASSIGN_OR_RETURN(LeafNode & leaf,
                   page_manager_.template Get<LeafNode>(root_id_));
  return Iterator(&page_manager_, &leaf, leaf.Size());
}

template <Trivial Key, Trivial Value, typename PagePool, typename Comparator,
          std::size_t max_keys, std::size_t max_elements>
absl::StatusOr<typename BTree<Key, Value, PagePool, Comparator, max_keys,
                              max_elements>::Iterator>
BTree<Key, Value, PagePool, Comparator, max_keys, max_elements>::Find(
    const Key& key) const {
  if (height_ > 0) {
    ASSIGN_OR_RETURN(InnerNode & inner,
                     page_manager_.template Get<InnerNode>(root_id_));
    ASSIGN_OR_RETURN((auto [leaf, pos]),
                     inner.Find(height_, key, page_manager_));
    if (leaf != nullptr) {
      return Iterator(&page_manager_, leaf, pos);
    }
    return End();
  }
  ASSIGN_OR_RETURN(LeafNode & leaf,
                   page_manager_.template Get<LeafNode>(root_id_));
  auto pos = leaf.Find(key);
  if (pos >= leaf.Size()) {
    return End();
  }
  return Iterator(&page_manager_, &leaf, pos);
}

template <Trivial Key, Trivial Value, typename PagePool, typename Comparator,
          std::size_t max_keys, std::size_t max_elements>
absl::StatusOr<bool>
BTree<Key, Value, PagePool, Comparator, max_keys, max_elements>::Insert(
    const Entry<Key, Value>& entry) {
  btree::InsertResult<Key> result;
  if (height_ > 0) {
    ASSIGN_OR_RETURN(InnerNode & inner,
                     page_manager_.template Get<InnerNode>(root_id_));
    ASSIGN_OR_RETURN(result,
                     inner.Insert(root_id_, height_, entry, page_manager_));
  } else {
    ASSIGN_OR_RETURN(LeafNode & leaf,
                     page_manager_.template Get<LeafNode>(root_id_));
    ASSIGN_OR_RETURN(result, leaf.Insert(root_id_, entry, page_manager_));
  }
  return std::visit(
      match{
          [&](btree::EntryPresent) -> absl::StatusOr<bool> { return false; },
          [&](btree::EntryAdded) -> absl::StatusOr<bool> {
            num_entries_++;
            return true;
          },
          [&](const btree::Split<Key>& split) -> absl::StatusOr<bool> {
            ASSIGN_OR_RETURN((auto [id, inner]),
                             page_manager_.template New<InnerNode>());
            page_manager_.MarkAsDirty(id);
            inner.Init(root_id_, split.key, split.new_tree);
            root_id_ = id;
            height_++;
            num_entries_++;
            return true;
          },
      },
      result);
}

template <Trivial Key, Trivial Value, typename PagePool, typename Comparator,
          std::size_t max_keys, std::size_t max_elements>
absl::Status
BTree<Key, Value, PagePool, Comparator, max_keys, max_elements>::Flush() {
  ASSIGN_OR_RETURN(MetaData & meta, page_manager_.template Get<MetaData>(0));
  meta.root = root_id_;
  meta.num_entries = num_entries_;
  meta.height = height_;
  page_manager_.MarkAsDirty(0);
  return page_manager_.Flush();
}

template <Trivial Key, Trivial Value, typename PagePool, typename Comparator,
          std::size_t max_keys, std::size_t max_elements>
absl::Status
BTree<Key, Value, PagePool, Comparator, max_keys, max_elements>::Close() {
  RETURN_IF_ERROR(Flush());
  return page_manager_.Close();
}

template <Trivial Key, Trivial Value, typename PagePool, typename Comparator,
          std::size_t max_keys, std::size_t max_elements>
absl::Status
BTree<Key, Value, PagePool, Comparator, max_keys, max_elements>::Check() const {
  if (height_ > 0) {
    ASSIGN_OR_RETURN(InnerNode & inner,
                     page_manager_.template Get<InnerNode>(root_id_));
    return inner.Check(height_, nullptr, nullptr, page_manager_);
  }
  ASSIGN_OR_RETURN(LeafNode & leaf,
                   page_manager_.template Get<LeafNode>(root_id_));
  return leaf.Check(nullptr, nullptr);
}

template <Trivial Key, Trivial Value, typename PagePool, typename Comparator,
          std::size_t max_keys, std::size_t max_elements>
void BTree<Key, Value, PagePool, Comparator, max_keys, max_elements>::Print()
    const {
  if (height_ > 0) {
    auto inner = page_manager_.template Get<InnerNode>(root_id_);
    if (!inner.ok()) {
      std::cout << "Unable to load root node: " << inner.status();
    }
    inner->get().Print(height_, 0, page_manager_);
    return;
  }
  auto leaf = page_manager_.template Get<LeafNode>(root_id_);
  if (!leaf.ok()) {
    std::cout << "Unable to load root node: " << leaf.status();
  }
  leaf->get().Print();
}

template <Trivial Key, Trivial Value, typename PagePool, typename Comparator,
          std::size_t max_keys, std::size_t max_elements>
const Entry<Key, Value>& BTree<Key, Value, PagePool, Comparator, max_keys,
                               max_elements>::Iterator::operator*() const {
  return node_->At(pos_);
}

template <Trivial Key, Trivial Value, typename PagePool, typename Comparator,
          std::size_t max_keys, std::size_t max_elements>
const Entry<Key, Value>* BTree<Key, Value, PagePool, Comparator, max_keys,
                               max_elements>::Iterator::get() const {
  return &(node_->At(pos_));
}

template <Trivial Key, Trivial Value, typename PagePool, typename Comparator,
          std::size_t max_keys, std::size_t max_elements>
absl::Status BTree<Key, Value, PagePool, Comparator, max_keys,
                   max_elements>::Iterator::Next() {
  if (pos_ < node_->Size() - 1) {
    pos_++;
    return absl::OkStatus();
  }
  PageId next = node_->GetSuccessor();
  if (next != 0) {
    ASSIGN_OR_RETURN(node_, manager_->template Get<LeafNode>(next));
    pos_ = 0;
  } else {
    pos_ = node_->Size();
  }
  return absl::OkStatus();
}

template <Trivial Key, Trivial Value, typename PagePool, typename Comparator,
          std::size_t max_keys, std::size_t max_elements>
absl::Status BTree<Key, Value, PagePool, Comparator, max_keys,
                   max_elements>::Iterator::Previous() {
  if (pos_ > 0) {
    pos_--;
    return absl::OkStatus();
  }
  PageId previous = node_->GetPredecessor();
  if (previous != 0) {
    ASSIGN_OR_RETURN(node_, manager_->template Get<LeafNode>(previous));
    pos_ = node_->Size() - 1;
  }
  return absl::OkStatus();
}

}  // namespace carmen::backend::btree
