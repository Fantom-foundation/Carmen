// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

#pragma once

#include "common/type.h"

namespace carmen {

// A configuration defines the implementation types of various primitives to be
// combined by schemas to instantiate a state.
template <template <typename K, typename V> class IndexType,
          template <typename K, typename V> class StoreType,
          template <typename K> class DepotType,
          template <typename K, typename V> class MultiMapType,
          typename ArchiveType>
struct Configuration {
  template <Trivial K, Trivial V>
  using Index = IndexType<K, V>;

  template <Trivial K, Trivial V>
  using Store = StoreType<K, V>;

  template <Trivial K>
  using Depot = DepotType<K>;

  template <Trivial K, Trivial V>
  using MultiMap = MultiMapType<K, V>;

  using Archive = ArchiveType;
};

}  // namespace carmen
