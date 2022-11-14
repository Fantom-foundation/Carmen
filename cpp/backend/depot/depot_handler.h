#pragma once

#include <cstddef>

#include "backend/depot/depot.h"
#include "backend/depot/leveldb/depot.h"
#include "backend/depot/file/depot.h"
#include "common/file_util.h"

namespace carmen::backend::depot {
namespace {

// A generic depot handler enclosing the setup and tear down of various depot
// implementations in benchmarks handled by depot_benchmark.cc. A handler holds
// an instance of a depot configured with given branching factor and number of
// boxes used for hashing.
//
// This generic DepotHandler is a mere wrapper on a depot reference, while
// specializations may add additional setup and tear-down operations.
template <Depot Depot, std::size_t branching_factor, std::size_t num_hash_boxes>
class DepotHandler {
 public:
  constexpr static std::size_t kBranchingFactor = branching_factor;
  constexpr static std::size_t kNumHashBoxes = num_hash_boxes;

  DepotHandler() : depot_(branching_factor, num_hash_boxes) {}
  Depot& GetDepot() { return depot_; }

 private:
  Depot depot_;
};

template <std::integral K, std::size_t branching_factor,
          std::size_t num_hash_boxes>
class DepotHandler<LevelDBDepot<K>, branching_factor, num_hash_boxes> {
 public:
  constexpr static std::size_t kBranchingFactor = branching_factor;
  constexpr static std::size_t kNumHashBoxes = num_hash_boxes;

  DepotHandler()
      : depot_(*LevelDBDepot<K>::Open(temp_dir_.GetPath(), branching_factor,
                                      num_hash_boxes)) {}
  LevelDBDepot<K>& GetDepot() { return depot_; }

 private:
  TempDir temp_dir_;
  LevelDBDepot<K> depot_;
};

template <std::integral K, std::size_t branching_factor,
          std::size_t num_hash_boxes>
class DepotHandler<FileDepot<K>, branching_factor, num_hash_boxes> {
 public:
  constexpr static std::size_t kBranchingFactor = branching_factor;
  constexpr static std::size_t kNumHashBoxes = num_hash_boxes;

  DepotHandler()
      : depot_(temp_dir_.GetPath(), branching_factor, num_hash_boxes) {}
  FileDepot<K>& GetDepot() { return depot_; }

 private:
  TempDir temp_dir_;
  FileDepot<K> depot_;
};

}  // namespace
}  // namespace carmen::backend::depot
