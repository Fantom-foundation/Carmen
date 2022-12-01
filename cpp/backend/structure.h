#pragma once

#include <concepts>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "common/memory_usage.h"
#include "common/type.h"

namespace carmen::backend {

namespace internal {

// These are concepts that are temporarily introduced until the migration to
// all-absl-Status based data structures is performed. After this, those
// concepts should be removed again.

template <typename T>
concept StatusOrHash =
    std::same_as<Hash, T> || std::same_as<absl::StatusOr<Hash>, T>;

template <typename T>
concept Void = std::same_as<void, T> || std::same_as<absl::Status, T>;

}  // namespace internal

// Defines universal requirements for all data strucutre implementations.
template <typename S>
concept Structure = requires(S a) {
  // Computes a hash over the full content of a data structure.
  { a.GetHash() } -> internal::StatusOrHash;
  // Structures must be flushable.
  { a.Flush() } -> internal::Void;
  // Structures must be closeable.
  { a.Close() } -> internal::Void;
}
// Structures must provide memory-footprint information.
&&MemoryFootprintProvider<S>;

}  // namespace carmen::backend
