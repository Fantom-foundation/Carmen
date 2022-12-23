#pragma once

#include <filesystem>

#include "absl/status/statusor.h"
#include "backend/common/btree/nodes.h"
#include "backend/common/page_manager.h"
#include "common/type.h"
#include "common/variant_util.h"

namespace carmen::backend {

// ----------------------------------------------------------------------------
//                         BTreeSet Definitions
// ----------------------------------------------------------------------------

// A BTreeSet is an ordered set of values stored on secondary storage. Each node
// of the tree is a page of a file. Inner nodes contain list of keys and
// child-page pointers, while leaf nodes contain only sorted list of values.
//
// This implementation can be customized by the type of value to be stored, the
// page pool implementation to be used for accessing data, and the order in
// which keys are stored. Also, to ease the testing of deeper trees, the default
// width of inner nodes and leafs can be overridden.
template <Trivial Value, typename PagePool,
          typename Comparator = std::less<Value>,
          std::size_t max_keys = 0,      // 0 means as many as fit in a page
          std::size_t max_elements = 0>  // 0 means as many as fit in a page
class BTreeSet {
 public:
  // Opens the set stored in the given directory. If no data is found, an empty
  // set is created.
  static absl::StatusOr<BTreeSet> Open(std::filesystem::path directory);

  // Tests whether the given element is contained in this set.
  absl::StatusOr<bool> Contains(const Value& value) const;

  // Inserts the given element.
  absl::StatusOr<bool> Insert(const Value& value);

  // For testing: checks internal invariants of this data structure.
  absl::Status Check() const;

  // For debugging: Prints the content of this tree to std::cout.
  void Print() const;

 private:
  using LeafNode = btree::LeafNode<Value, Comparator, max_keys>;
  using InnerNode = btree::InnerNode<LeafNode, max_elements>;

  BTreeSet();

  // The page ID of the root node.
  PageId root_id_ = 0;

  // The total number of elements stored in this tree.
  std::uint64_t num_elements_ = 0;

  // The node height of this tree. This is the maximum number of nodes that need
  // to be accessed when navigating from the root to the leaf nodes.
  std::uint32_t height_ = 0;

  // The page manager handling the allocation of nodes (=pages).
  PageManager<PagePool> page_manager_;
};

// ----------------------------------------------------------------------------
//                           BTreeSet Definitions
// ----------------------------------------------------------------------------

template <Trivial Value, typename PagePool, typename Comparator,
          std::size_t max_keys, std::size_t max_elements>
absl::StatusOr<BTreeSet<Value, PagePool, Comparator, max_keys, max_elements>>
BTreeSet<Value, PagePool, Comparator, max_keys, max_elements>::Open(
    std::filesystem::path) {
  // TODO: support loading actual data from the file.
  return BTreeSet();
}

template <Trivial Value, typename PagePool, typename Comparator,
          std::size_t max_keys, std::size_t max_elements>
BTreeSet<Value, PagePool, Comparator, max_keys, max_elements>::BTreeSet() {
  // Start with initially empty root page.
  page_manager_.template New<LeafNode>().IgnoreError();
}

template <Trivial Value, typename PagePool, typename Comparator,
          std::size_t max_keys, std::size_t max_elements>
absl::StatusOr<bool>
BTreeSet<Value, PagePool, Comparator, max_keys, max_elements>::Contains(
    const Value& value) const {
  if (height_ > 0) {
    ASSIGN_OR_RETURN(InnerNode & inner,
                     page_manager_.template Get<InnerNode>(root_id_));
    return inner.Contains(height_, value, page_manager_);
  }
  ASSIGN_OR_RETURN(LeafNode & leaf,
                   page_manager_.template Get<LeafNode>(root_id_));
  return leaf.Contains(value);
}

template <Trivial Value, typename PagePool, typename Comparator,
          std::size_t max_keys, std::size_t max_elements>
absl::StatusOr<bool> BTreeSet<Value, PagePool, Comparator, max_keys,
                              max_elements>::Insert(const Value& value) {
  btree::InsertResult<Value> result;
  if (height_ > 0) {
    ASSIGN_OR_RETURN(InnerNode & inner,
                     page_manager_.template Get<InnerNode>(root_id_));
    ASSIGN_OR_RETURN(result, inner.Insert(height_, value, page_manager_));
  } else {
    ASSIGN_OR_RETURN(LeafNode & leaf,
                     page_manager_.template Get<LeafNode>(root_id_));
    ASSIGN_OR_RETURN(result, leaf.Insert(value, page_manager_));
  }
  return std::visit(
      match{
          [&](btree::ElementPresent) -> absl::StatusOr<bool> { return false; },
          [&](btree::ElementAdded) -> absl::StatusOr<bool> {
            num_elements_++;
            return true;
          },
          [&](const btree::Split<Value>& split) -> absl::StatusOr<bool> {
            ASSIGN_OR_RETURN((auto [id, inner]),
                             page_manager_.template New<InnerNode>());
            inner.Init(root_id_, split.key, split.new_tree);
            root_id_ = id;
            height_++;
            num_elements_++;
            return true;
          },
      },
      result);
}

template <Trivial Value, typename PagePool, typename Comparator,
          std::size_t max_keys, std::size_t max_elements>
absl::Status
BTreeSet<Value, PagePool, Comparator, max_keys, max_elements>::Check() const {
  if (height_ > 0) {
    ASSIGN_OR_RETURN(InnerNode & inner,
                     page_manager_.template Get<InnerNode>(root_id_));
    return inner.Check(height_, nullptr, nullptr, page_manager_);
  }
  ASSIGN_OR_RETURN(LeafNode & leaf,
                   page_manager_.template Get<LeafNode>(root_id_));
  return leaf.Check(nullptr, nullptr);
}

template <Trivial Value, typename PagePool, typename Comparator,
          std::size_t max_keys, std::size_t max_elements>
void BTreeSet<Value, PagePool, Comparator, max_keys, max_elements>::Print()
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

}  // namespace carmen::backend
