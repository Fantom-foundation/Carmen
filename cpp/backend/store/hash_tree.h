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

#include <algorithm>
#include <filesystem>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "backend/common/leveldb/leveldb.h"
#include "backend/common/page_id.h"
#include "common/hash.h"
#include "common/memory_usage.h"
#include "common/type.h"

namespace carmen::backend::store {

// An interface for a source of page data if needed by the HashTree.
class PageSource {
 public:
  virtual ~PageSource(){};
  // Requests a view on the data of the given page.
  virtual absl::StatusOr<std::span<const std::byte>> GetPageData(PageId id) = 0;
};

// A HashTree is managing the hashes of a list of pages as well as the
// aggregation thereof to a single global hash.
//
// This class maintains a hierarchy of partially aggregated page hashes,
// as well as dirty state information. Whenever a full hash is requested, dirty
// (=outdated) hashes are refreshed, before a new global hash is obtained.
class HashTree {
 public:
  // Creates a new hash tree using the given source for fetching page data
  // whenever needed. The provided branching factor is used for the recursive
  // computation of an aggregated hash over all pages. A value of 32 implies
  // that 32 hashes of one level are combined into a single hash on the next
  // level. The first level with a single hash defines the overall hash.
  HashTree(std::unique_ptr<PageSource> source, int branching_factor = 32)
      : branching_factor_(branching_factor), page_source_(std::move(source)) {}

  // Informs the HashTree about the existence of the given page. This may lead
  // to and adaptation of the internal hash data structures and dirty pages.
  // Note: registration is implicit for all other operations on pages.
  void RegisterPage(PageId id);

  // Updates the hash of a single page. Use this if hash computation has been
  // performed for some reason, and the result can be used by the HashTree.
  // After the call, the hash of the given page is considered up to date.
  void UpdateHash(PageId id, const Hash& hash);

  // A variant of the function above, where the hash of the page is computed
  // within the function. Use this variant in cases where pages are about to be
  // discarded and later fetching would require more costly operations (e.g.
  // during page eviction).
  void UpdateHash(PageId id, std::span<const std::byte> page);

  // Marks the given page as being modified. Consequently, the page's hash will
  // have to be recomputed the next time a global hash is requested.
  void MarkDirty(PageId page);

  // Computes a global hash for all pages managed by this HashTree. It will
  // update outdated partial hashes cached internally, which may imply the need
  // for fetching dirty pages.
  absl::StatusOr<Hash> GetHash();

  // Saves the hashes of this tree into the given file. Before saving them, all
  // outdated hashes are implicitly refreshed.
  absl::Status SaveToFile(const std::filesystem::path& file);

  // Discards the current content of this HashTree and loads all hashes from the
  // given file. Loaded hashes are considered up-to-date. After loading, the
  // internal tree structure is updated, and the file verified for consistency.
  // Returns true if the loading was successful, false otherwise.
  absl::Status LoadFromFile(const std::filesystem::path& file);

  // Saves the hashes of this tree into the given leveldb path. Before saving
  // them, all outdated hashes are implicitly refreshed.
  absl::Status SaveToLevelDb(const std::filesystem::path& path);

  // Saves the hashes of this tree into the given leveldb instance. Before
  // saving them, all outdated hashes are implicitly refreshed.
  absl::Status SaveToLevelDb(LevelDb& leveldb);

  // Discards the current content of this HashTree and loads all hashes from the
  // given leveldb path. Loaded hashes are considered up-to-date. After loading,
  // the internal tree structure is updated, and the file verified for
  // consistency.
  absl::Status LoadFromLevelDb(const std::filesystem::path& path);

  // Discards the current content of this HashTree and loads all hashes from the
  // given leveldb instance. Loaded hashes are considered up-to-date. After
  // loading, the internal tree structure is updated, and the file verified for
  // consistency.
  absl::Status LoadFromLevelDb(const LevelDb& leveldb);

  // Computes a summary of the memory usage of this tree.
  MemoryFootprint GetMemoryFootprint() const;

 private:
  // Fetches the hashes of a given layer of the reduction tree. If the layer
  // does not exist, it is created.
  std::vector<Hash>& GetHashes(std::size_t level);

  // Fetches the hash value for a given level / position in the reduction tree.
  // If the position does not exist, it is created.
  Hash& GetMutableHash(std::size_t level, std::size_t pos);

  // Keeps track of the total number of managed pages. Used internally whenever
  // new pages may be added.
  void TrackNumPages(PageId page);

  // The branching factor used by the recursive hash aggregation algorithm.
  const std::size_t branching_factor_;

  Sha256Hasher hasher_;
  std::vector<std::vector<Hash>> hashes_;
  std::size_t num_pages_ = 0;

  std::unique_ptr<PageSource> page_source_;
  absl::flat_hash_set<PageId> dirty_pages_;
  absl::flat_hash_set<int> dirty_level_one_positions_;
};

}  // namespace carmen::backend::store
