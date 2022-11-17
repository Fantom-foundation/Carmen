#pragma once

#include <cstddef>

#include "backend/depot/depot.h"
#include "backend/depot/file/depot.h"
#include "backend/depot/leveldb/depot.h"
#include "common/file_util.h"

namespace carmen::backend::depot {
namespace {

// The reference depot implementation type used to validate implementations.
template <std::integral K>
using ReferenceDepot = InMemoryDepot<K>;

// A base type for DepotHandlerBase types (see below) exposing common
// definitions.
template <std::integral K, std::size_t branching_factor,
          std::size_t num_hash_boxes>
class DepotHandlerBase {
 public:
  constexpr static std::size_t kBranchingFactor = branching_factor;
  constexpr static std::size_t kNumHashBoxes = num_hash_boxes;

  // Obtains access to a reference depot implementation to be used to compare
  // the handled depot with.
  DepotHandlerBase() : reference_(branching_factor, num_hash_boxes) {}

  auto& GetReferenceDepot() { return reference_; }

  std::filesystem::path GetDepotDirectory() const { return temp_dir_; }

 private:
  TempDir temp_dir_;
  ReferenceDepot<K> reference_;
};

// A generic depot handler enclosing the setup and tear down of various depot
// implementations in benchmarks handled by depot_benchmark.cc. A handler holds
// an instance of a depot configured with given branching factor and number of
// boxes used for hashing.
//
// This generic DepotHandler is a mere wrapper on a depot reference, while
// specializations may add additional setup and tear-down operations.
template <Depot Depot, std::size_t branching_factor, std::size_t num_hash_boxes>
class DepotHandler : public DepotHandlerBase<typename Depot::key_type,
                                             branching_factor, num_hash_boxes> {
 public:
  using DepotHandlerBase<typename Depot::key_type, branching_factor,
                         num_hash_boxes>::GetDepotDirectory;
  DepotHandler()
      : depot_(*Depot::Open(GetDepotDirectory(), branching_factor,
                            num_hash_boxes)) {}
  Depot& GetDepot() { return depot_; }

 private:
  Depot depot_;
};

// A specialization of a DepotHandler for InMemoryDepot handling ingoring the
// creation/deletion of temporary files and directories.
template <std::integral K, std::size_t branching_factor,
          std::size_t num_hash_boxes>
class DepotHandler<InMemoryDepot<K>, branching_factor, num_hash_boxes>
    : public DepotHandlerBase<K, branching_factor, num_hash_boxes> {
 public:
  DepotHandler() : depot_(branching_factor, num_hash_boxes) {}
  InMemoryDepot<K>& GetDepot() { return depot_; }

 private:
  InMemoryDepot<K> depot_;
};

}  // namespace
}  // namespace carmen::backend::depot
