#pragma once

#include <concepts>
#include <cstddef>
#include <span>
#include <type_traits>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
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
  // Computes a hash over the full content of this depot.
  { b.GetHash() } -> std::same_as<absl::StatusOr<Hash>>;
  // Depots must be flushable.
  { a.Flush() } -> std::same_as<absl::Status>;
  // Depots must be closeable.
  { a.Close() } -> std::same_as<absl::Status>;
}
&&MemoryFootprintProvider<D>;

}  // namespace carmen::backend::depot
