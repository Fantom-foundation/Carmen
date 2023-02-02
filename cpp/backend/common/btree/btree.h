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
  // Returns the value associated to the given key or std::nullopt if there is
  // no such key. To mearly check whether an element is present it is more
  // efficient to use the Contains(..) function, since contains may stop in the
  // unlikely case of the key being present in an inner node, while find needs
  // to go all the way to the leaf node to get the value.
  absl::StatusOr<std::optional<Value>> Find(const Key& key) const;

  // Inserts the given entry. This function is intended to be used by derived
  // implementations, customized for their use case.
  absl::StatusOr<bool> Insert(const Entry<Key, Value>& entry);

 private:
  using LeafNode = btree::LeafNode<Key, Value, Comparator, max_keys>;
  using InnerNode = btree::InnerNode<LeafNode, max_elements>;

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
absl::StatusOr<std::optional<Value>>
BTree<Key, Value, PagePool, Comparator, max_keys, max_elements>::Find(
    const Key& key) const {
  if (height_ > 0) {
    ASSIGN_OR_RETURN(InnerNode & inner,
                     page_manager_.template Get<InnerNode>(root_id_));
    return inner.Find(height_, key, page_manager_);
  }
  ASSIGN_OR_RETURN(LeafNode & leaf,
                   page_manager_.template Get<LeafNode>(root_id_));
  return leaf.Find(key);
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

}  // namespace carmen::backend::btree
