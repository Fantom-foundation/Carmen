#pragma once

#include <concepts>
#include <filesystem>
#include <type_traits>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "common/heterogenous_map.h"
#include "common/memory_usage.h"
#include "common/type.h"

namespace carmen::backend {

// A context provides a common environment for a group of data structures
// that are intended to be used together, for instance in a combined State
// involving multiple indexes, stores, and depots. It is mainly intended to
// provide access to shared components like a page pools or other resources.
// It is also intended to contain runtime configuration parameters.
class Context {
 public:
  // Tests whether a component of the given type has been registered before.
  template <typename T>
  bool HasComponent() const {
    return components_.Contains<T>();
  }

  // Retrieves a component of the given type which must have been registered
  // before.
  template <typename T>
  T& GetComponent() {
    assert(HasComponent<T>());
    return components_.Get<T>();
  }

  // Registers a component with the given type.
  template <typename T>
  void RegisterComponent(T component) {
    components_.Set<T>(std::move(component));
  }

 private:
  HeterogenousMap components_;
};

// Defines universal requirements for all data structure implementations.
template <typename S>
concept Structure = requires(S a) {
  // All data structures must be open-able through a static factory function.
  // The provided context can be used to share elements between structures.
  {
    S::Open(std::declval<Context&>(),
            std::declval<const std::filesystem::path&>())
    } -> std::same_as<absl::StatusOr<S>>;
  // Structures must be flushable.
  { a.Flush() } -> std::same_as<absl::Status>;
  // Structures must be closeable.
  { a.Close() } -> std::same_as<absl::Status>;
}
// All structures must be moveable.
&&std::is_move_constructible_v<S>
    // Structures must provide memory-footprint information.
    && MemoryFootprintProvider<S>;

// Extends the requiremetns of a data structure by an additional need for
// supporting effective full-state hashing.
template <typename S>
concept HashableStructure = Structure<S> && requires(S a) {
  // Computes a hash over the full content of a data structure.
  { a.GetHash() } -> std::same_as<absl::StatusOr<Hash>>;
};

}  // namespace carmen::backend
