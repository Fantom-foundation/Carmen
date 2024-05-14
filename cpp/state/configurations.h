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

#include "archive/archive.h"
#include "backend/depot/file/depot.h"
#include "backend/depot/leveldb/depot.h"
#include "backend/depot/memory/depot.h"
#include "backend/index/cache/cache.h"
#include "backend/index/file/index.h"
#include "backend/index/leveldb/multi_db/index.h"
#include "backend/index/memory/index.h"
#include "backend/multimap/memory/multimap.h"
#include "backend/store/file/store.h"
#include "backend/store/leveldb/store.h"
#include "backend/store/memory/store.h"
#include "state/configuration.h"

namespace carmen {

constexpr const std::size_t kPageSize = 1 << 12;  // 4 KiB

// ----------------------------------------------------------------------------
//                         In-Memory Configuration
// ----------------------------------------------------------------------------

template <typename K, typename V>
using InMemoryIndex = backend::index::InMemoryIndex<K, V>;

template <typename K, typename V>
using InMemoryStore = backend::store::InMemoryStore<K, V, kPageSize>;

template <typename K>
using InMemoryDepot = backend::depot::InMemoryDepot<K>;

template <typename K, typename V>
using InMemoryMultiMap = backend::multimap::InMemoryMultiMap<K, V>;

template <Archive Archive>
using InMemoryConfig = Configuration<InMemoryIndex, InMemoryStore,
                                     InMemoryDepot, InMemoryMultiMap, Archive>;

// ----------------------------------------------------------------------------
//                         File-Based Configuration
// ----------------------------------------------------------------------------

template <typename K, typename I>
using FileBasedIndex = backend::index::Cached<
    backend::index::FileIndex<K, I, backend::SingleFile, kPageSize>>;

template <typename K, typename V>
using FileBasedStore =
    backend::store::EagerFileStore<K, V, backend::SingleFile, kPageSize>;

template <typename K>
using FileBasedDepot = backend::depot::FileDepot<K>;

template <Archive Archive>
using FileBasedConfig =
    Configuration<FileBasedIndex, FileBasedStore, FileBasedDepot,
                  InMemoryMultiMap, Archive>;

// ----------------------------------------------------------------------------
//                         LevelDB-Based Configuration
// ----------------------------------------------------------------------------

template <typename K, typename I>
using LevelDbBasedIndex =
    backend::index::Cached<backend::index::MultiLevelDbIndex<K, I>>;

template <typename K, typename V>
using LevelDbBasedStore = backend::store::LevelDbStore<K, V, kPageSize>;

template <typename K>
using LevelDbBasedDepot = backend::depot::LevelDbDepot<K>;

template <Archive Archive>
using LevelDbBasedConfig =
    Configuration<LevelDbBasedIndex, LevelDbBasedStore, LevelDbBasedDepot,
                  InMemoryMultiMap, Archive>;

}  // namespace carmen
