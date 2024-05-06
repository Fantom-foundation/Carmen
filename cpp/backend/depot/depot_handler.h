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

#include <cstddef>

#include "absl/status/statusor.h"
#include "backend/depot/cache/cache.h"
#include "backend/depot/depot.h"
#include "backend/depot/memory/depot.h"
#include "common/file_util.h"
#include "common/status_util.h"

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

 private:
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
  template <typename... Args>
  static absl::StatusOr<DepotHandler> Create(Args&&... args) {
    TempDir dir;
    ASSIGN_OR_RETURN(auto depot,
                     Depot::Open(dir.GetPath(), branching_factor, hash_box_size,
                                 std::forward<Args>(args)...));
    return DepotHandler(std::move(depot), std::move(dir));
  }

  Depot& GetDepot() { return depot_; }

 private:
  DepotHandler(Depot depot, TempDir dir)
      : temp_dir_(std::move(dir)), depot_(std::move(depot)) {}

  TempDir temp_dir_;
  Depot depot_;
};

// A specialization of a DepotHandler for Cached depot handling ignoring the
// creation/deletion of temporary files and directories.
template <Depot Depot, std::size_t branching_factor, std::size_t hash_box_size>
class DepotHandler<Cached<Depot>, branching_factor, hash_box_size>
    : public DepotHandlerBase<typename Depot::key_type, branching_factor,
                              hash_box_size> {
 public:
  template <typename... Args>
  static absl::StatusOr<DepotHandler> Create(Args&&... args) {
    TempDir dir;
    ASSIGN_OR_RETURN(auto depot,
                     Depot::Open(dir.GetPath(), branching_factor, hash_box_size,
                                 std::forward<Args>(args)...));
    return DepotHandler(std::move(depot), std::move(dir));
  }

  Cached<Depot>& GetDepot() { return depot_; }

 private:
  DepotHandler(Depot nested, TempDir dir)
      : temp_dir_(std::move(dir)), depot_(std::move(nested)) {}

  TempDir temp_dir_;
  Cached<Depot> depot_;
};

}  // namespace
}  // namespace carmen::backend::depot
