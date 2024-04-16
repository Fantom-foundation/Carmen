/*
 * Copyright (c) 2024 Fantom Foundation
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at fantom.foundation/bsl11.
 *
 * Change Date: 2028-4-16
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by the GNU Lesser General Public Licence v3 
 */

#pragma once

#include <concepts>
#include <cstddef>
#include <span>
#include <type_traits>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "backend/structure.h"
#include "common/memory_usage.h"
#include "common/type.h"

namespace carmen::backend::depot {

// Defines the interface expected for a Depot D mapping integral keys to
// byte array values of various lengths.
template <typename D>
concept Depot = requires(D a, const D b) {
  // A depot must expose a key type.
  std::integral<typename D::key_type>;

  // Depots must be movable.
  std::is_move_constructible_v<D>;
  std::is_move_assignable_v<D>;

  // Set data for given key.
  {
    a.Set(std::declval<typename D::key_type>(),
          std::declval<std::span<const std::byte>>())
    } -> std::same_as<absl::Status>;
  // Retrieves data from Depot. Not found status is returned when not found.
  {
    b.Get(std::declval<typename D::key_type>())
    } -> std::same_as<absl::StatusOr<std::span<const std::byte>>>;
  // Retrieves size of data from Depot. Not found status is returned when not
  {
    b.GetSize(std::declval<typename D::key_type>())
    } -> std::same_as<absl::StatusOr<std::uint32_t>>;
}
// Depots must satisfy the requirements for backend data structures.
&&HashableStructure<D>;

}  // namespace carmen::backend::depot
