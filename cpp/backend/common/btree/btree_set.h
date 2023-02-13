#pragma once

#include <filesystem>

#include "absl/status/statusor.h"
#include "backend/common/btree/btree.h"
#include "backend/common/btree/entry.h"
#include "backend/common/btree/nodes.h"
#include "backend/common/page_manager.h"
#include "common/type.h"
#include "common/variant_util.h"

namespace carmen::backend {

// ----------------------------------------------------------------------------
//                         BTreeSet Declaration
// ----------------------------------------------------------------------------

// A BTreeSet is an ordered set of values stored on secondary storage. Each node
// of the tree is a page of a file. Inner nodes contain list of values used as
// keys and child-page pointers, while leaf nodes contain only sorted list of
// values. Keys stored in inner nodes are not repeated in leave nodes.
//
// This implementation can be customized by the type of value to be stored, the
// page pool implementation to be used for accessing data, and the order in
// which keys are stored. Also, to ease the testing of deeper trees, the default
// width of inner nodes and leafs can be overridden.
template <Trivial Value, typename PagePool,
          typename Comparator = std::less<Value>,
          std::size_t max_keys = 0,      // 0 means as many as fit in a page
          std::size_t max_elements = 0>  // 0 means as many as fit in a page
class BTreeSet : public btree::BTree<Value, btree::Unit, PagePool, Comparator,
                                     max_keys, max_elements> {
  using super = btree::BTree<Value, btree::Unit, PagePool, Comparator, max_keys,
                             max_elements>;

 public:
  // An iterator for sets. It is a customized version of the iterator offered
  // for maps hidding the implicit Unit value.
  class Iterator : public super::Iterator {
   public:
    Iterator() = default;

    // Adapts the base iterator to only provide access to the key.
    const Value& operator*() const { return super::Iterator::operator*().key; }

   private:
    friend class BTreeSet;
    Iterator(typename super::Iterator base) : super::Iterator(base) {}
  };

  // Opens the set stored in the given directory. If no data is found, an empty
  // set is created.
  static absl::StatusOr<BTreeSet> Open(std::filesystem::path directory) {
    return super::template Open<BTreeSet>(directory);
  }

  absl::StatusOr<Iterator> Begin() const {
    ASSIGN_OR_RETURN(auto iter, super::Begin());
    return Iterator(std::move(iter));
  }

  absl::StatusOr<Iterator> End() const {
    ASSIGN_OR_RETURN(auto iter, super::End());
    return Iterator(std::move(iter));
  }

  absl::StatusOr<Iterator> Find(const Value& value) const {
    ASSIGN_OR_RETURN(auto iter, super::Find(value));
    return Iterator(std::move(iter));
  }

  // Inserts the given element.
  absl::StatusOr<bool> Insert(const Value& value) {
    return super::Insert(value);
  }

 private:
  // Inherit the constructors of the generic BTree implementation.
  using btree::BTree<Value, btree::Unit, PagePool, Comparator, max_keys,
                     max_elements>::BTree;
};

}  // namespace carmen::backend
