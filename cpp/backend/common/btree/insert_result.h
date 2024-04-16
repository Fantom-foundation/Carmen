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
//   - the entry was present, no insert occured
//   - the entry was added, no split necessary
//   - the entry was added, but this triggered a split that needs to be
//     handled by the parent

// The result returned if an entry with the same key was already present.
struct EntryPresent {
  auto operator<=>(const EntryPresent&) const = default;
};

// The result returned if an entry was added without a split.
struct EntryAdded {
  auto operator<=>(const EntryAdded&) const = default;
};

// The result returned when an insertion triggered a split.
template <typename Key>
struct Split {
  auto operator<=>(const Split<Key>&) const = default;
  // The key to be used to in the parent node to distinguish between the node
  // the element was inserted and the new tree returned. Both, the key and the
  // new tree should be inserted in one of the anchester nodes.
  Key key;
  // The PageId of the root of the tree created by the split, to be inserted in
  // some parent node.
  PageId new_tree;

  // Make splits human readable in test assertions.
  friend std::ostream& operator<<(std::ostream& out,
                                  const btree::Split<Key>& split) {
    return out << "Split{" << split.key << "," << split.new_tree << "}";
  }
};

// The type combining the possible insert results.
template <typename Key>
using InsertResult = std::variant<EntryPresent, EntryAdded, Split<Key>>;

}  // namespace carmen::backend::btree
