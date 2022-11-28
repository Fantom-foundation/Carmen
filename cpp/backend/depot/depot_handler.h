#pragma once

#include <cstddef>

#include "backend/depot/depot.h"
#include "backend/depot/memory/depot.h"
#include "common/file_util.h"

namespace carmen::backend::depot {
namespace {

// The reference depot implementation type used to validate implementations.
template <std::integral K>
using ReferenceDepot = InMemoryDepot<K>;

// A base type for DepotHandlerBase types (see below) exposing common
// definitions.
template <std::integral K, std::size_t branching_factor,
          std::size_t hash_box_size>
class DepotHandlerBase {
 public:
  constexpr static std::size_t kBranchingFactor = branching_factor;
  constexpr static std::size_t kHashBoxSize = hash_box_size;

  // Obtains access to a reference depot implementation to be used to compare
  // the handled depot with.
  DepotHandlerBase() : reference_(branching_factor, hash_box_size) {}

  auto& GetReferenceDepot() { return reference_; }

  std::filesystem::path GetDepotDirectory() const { return temp_dir_; }

 private:
  TempDir temp_dir_;
  ReferenceDepot<K> reference_;
};

// A generic depot handler enclosing the setup and tear down of various depot
// implementations in benchmarks handled by depot_benchmark.cc. A handler holds
// an instance of a depot configured with given branching factor and number of
// items used for hashing.
//
// This generic DepotHandler is a mere wrapper on a depot reference, while
// specializations may add additional setup and tear-down operations.
template <Depot Depot, std::size_t branching_factor, std::size_t hash_box_size>
class DepotHandler : public DepotHandlerBase<typename Depot::key_type,
                                             branching_factor, hash_box_size> {
 public:
  using DepotHandlerBase<typename Depot::key_type, branching_factor,
                         hash_box_size>::GetDepotDirectory;
  DepotHandler()
      : depot_(*Depot::Open(GetDepotDirectory(), branching_factor,
                            hash_box_size)) {}
  Depot& GetDepot() { return depot_; }

 private:
  Depot depot_;
};

// A specialization of a DepotHandler for InMemoryDepot handling ignoring the
// creation/deletion of temporary files and directories.
template <std::integral K, std::size_t branching_factor,
          std::size_t hash_box_size>
class DepotHandler<InMemoryDepot<K>, branching_factor, hash_box_size>
    : public DepotHandlerBase<K, branching_factor, hash_box_size> {
 public:
  DepotHandler() : depot_(branching_factor, hash_box_size) {}
  InMemoryDepot<K>& GetDepot() { return depot_; }

 private:
  InMemoryDepot<K> depot_;
};

}  // namespace
}  // namespace carmen::backend::depot
