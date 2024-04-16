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

#include <filesystem>

#include "absl/status/statusor.h"
#include "backend/common/btree/btree.h"
#include "backend/common/btree/nodes.h"
#include "backend/common/page_manager.h"
#include "common/type.h"
#include "common/variant_util.h"

namespace carmen::backend {

// ----------------------------------------------------------------------------
//                         BTreeMap Declaration
// ----------------------------------------------------------------------------

// A BTreeMap is an ordered map of key/value pairs stored on secondary storage.
// Each node of the tree is a page of a file. Inner nodes contain list of keys
// and child-page pointers, while leaf nodes contain key/value pairs.
//
// This implementation can be customized by the type of key/values to be stored,
// the page pool implementation to be used for accessing data, and the order in
// which keys are stored. Also, to ease the testing of deeper trees, the default
// width of inner nodes and leafs can be overridden.
template <Trivial Key, Trivial Value, typename PagePool,
          typename Comparator = std::less<Key>,
          std::size_t max_keys = 0,      // 0 means as many as fit in a page
          std::size_t max_elements = 0>  // 0 means as many as fit in a page
class BTreeMap : public btree::BTree<Key, Value, PagePool, Comparator, max_keys,
                                     max_elements> {
  using super =
      btree::BTree<Key, Value, PagePool, Comparator, max_keys, max_elements>;
  using entry_t = btree::Entry<Key, Value>;

 public:
  // Opens the map stored in the given directory. If no data is found, an empty
  // map is created.
  static absl::StatusOr<BTreeMap> Open(std::filesystem::path directory) {
    return super::template Open<BTreeMap>(directory);
  }

  // Inserts the given key/value pair into this map. If the given key is not yet
  // present, it will be added, mapped to the given value. If the given key is
  // present, the content of the map is not changed. Returns true if an element
  // was inserted, false otherwise.
  absl::StatusOr<bool> Insert(const Key& key, const Value& value) {
    return super::Insert(entry_t{key, value});
  }

  // Attempts to locate the given key in the map and returns the associated
  // value, or std::nullopt if there is no such key in the map.
  using super::Find;

  // Make the BTree's Begin and End members public accessible.
  using super::Begin;
  using super::End;

 private:
  // Inherit the constructors of the generic BTree implementation.
  using super::BTree;
};

}  // namespace carmen::backend
