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
