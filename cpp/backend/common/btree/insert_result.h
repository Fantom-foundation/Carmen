#pragma once

#include <ostream>
#include <variant>

#include "backend/common/page_id.h"

namespace carmen::backend::btree {

// ----------------------------------------------------------------------------
//                              Definition
// ----------------------------------------------------------------------------

// This file defines the type returned by recursive internal BTree insert
// operations indicating the effect of the insertion to the parent. This could
// be one of the following cases:
//   - the element was present, no insert occured
//   - the element was added, no split necessary
//   - the element was added, but this triggered a split that needs to be
//     handled by the parent

// This is the type of result returned if an inserted element was present.
struct ElementPresent {
  auto operator<=>(const ElementPresent&) const = default;
};

// This is the type of result returned if an element was added without a split.
struct ElementAdded {
  auto operator<=>(const ElementAdded&) const = default;
};

// This is the type of result returned when an insertion triggered a split.
template <typename V>
struct Split {
  auto operator<=>(const Split<V>&) const = default;
  // The key to be used to in the parent node to distinguish between the node
  // the element was inserted and the new tree returned. Both, the key and the
  // new tree should be inserted in one of the anchester nodes.
  V key;
  // The PageId of the root of the tree created by the split, to be inserted in
  // some parent node.
  PageId new_tree;

  // Make splits human readable in test assertions.
  friend std::ostream& operator<<(std::ostream& out,
                                  const btree::Split<V>& split) {
    return out << "Split{" << split.key << "," << split.new_tree << "}";
  }
};

// The type combining the possible insert results.
template <typename V>
using InsertResult = std::variant<ElementPresent, ElementAdded, Split<V>>;

}  // namespace carmen::backend::btree
