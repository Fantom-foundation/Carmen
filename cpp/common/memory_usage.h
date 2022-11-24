#pragma once

#include <cstdint>
#include <deque>
#include <istream>
#include <ostream>
#include <queue>
#include <string>
#include <string_view>
#include <typeinfo>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/functional/function_ref.h"
#include "absl/status/statusor.h"

namespace carmen {

// A type describing an amount of memory in a type-safe way.
// Memory can be compared, assigned, added, subtracted and scaled.
class Memory {
 public:
  explicit constexpr Memory(uint64_t bytes = 0) : bytes_(bytes) {}
  auto operator<=>(const Memory&) const = default;
  constexpr std::int64_t bytes() const { return bytes_; }

  Memory& operator+=(const Memory&);
  Memory& operator-=(const Memory&);
  Memory& operator*=(int factor);

 private:
  std::int64_t bytes_;
};

template <typename T>
Memory SizeOf() {
  return Memory(sizeof(T));
}

// Approximates the memory usage of the given vector assuming the element type
// is a stack-only type.
template <typename T>
Memory SizeOf(const std::vector<T>& vector) {
  return SizeOf<T>() * vector.size();
}

// Approximates the memory usage of the given queue assuming the element type is
// a stack-only type.
template <typename T>
Memory SizeOf(const std::queue<T>& list) {
  return SizeOf<T>() * list.size();
}

// Approximates the memory usage of the given deque assuming the element type is
// a stack-only type.
template <typename T>
Memory SizeOf(const std::deque<T>& list) {
  return SizeOf<T>() * list.size();
}

// Approximates the memory usage of the given set assuming the element type is a
// stack-only type.
template <typename T>
Memory SizeOf(const absl::flat_hash_set<T>& set) {
  return SizeOf<T>() * set.size();
}

// Approximates the memory usage of the given map assuming the key and value
// types are a stack-only types.
template <typename K, typename V>
Memory SizeOf(const absl::flat_hash_map<K, V>& map) {
  return (SizeOf<K>() + SizeOf<V>()) * map.size();
}

constexpr Memory operator+(const Memory& a, const Memory& b) {
  return Memory(a.bytes() + b.bytes());
}

constexpr Memory operator-(const Memory& a, const Memory& b) {
  return Memory(a.bytes() - b.bytes());
}

constexpr Memory operator*(const Memory& a, int factor) {
  return Memory(a.bytes() * factor);
}

constexpr Memory operator*(int factor, const Memory& a) { return a * factor; }

std::ostream& operator<<(std::ostream& out, const Memory& memory);

// Some memory constants.
constexpr static const Memory Byte(1);
constexpr static const Memory KiB = 1024 * Byte;
constexpr static const Memory MiB = 1024 * KiB;
constexpr static const Memory GiB = 1024 * MiB;
constexpr static const Memory TiB = 1024 * GiB;
constexpr static const Memory PiB = 1024 * TiB;
constexpr static const Memory EiB = 1024 * PiB;

// A MemoryFootprint describes the memory usage of a DAG shaped object graph.
// Each node is the root of a DAG of objects, where each node is a object
// desribed by a MemoryFootprint instance including its memory usage, and each
// edge is labeled by a field name.
class MemoryFootprint {
 public:
  // Initializes a memory footprint description for the given object.
  template <typename T>
  MemoryFootprint(const T& obj)
      : MemoryFootprint({&obj, &typeid(T)}, SizeOf<T>()) {}

  // Creates a new memory footprint object describing the memory usage of a
  // unique object.
  MemoryFootprint(Memory self = Memory()) : MemoryFootprint(kUnique, self) {}

  // Computes the total memory footprint of the DAG rooted by this node.
  Memory GetTotal() const;

  // Adds a field with the given footprint to this node.
  MemoryFootprint& Add(std::string_view label, MemoryFootprint footprint);

  // Loads a memory footprint from the given stream.
  static absl::StatusOr<MemoryFootprint> ReadFrom(std::istream& in);

  // Writes a binary version of this object to the given stream.
  std::ostream& WriteTo(std::ostream& out) const;

  // Prints a summary of the memory described memory usage.
  friend std::ostream& operator<<(std::ostream& out, const MemoryFootprint&);

 private:
  // An object is described by its location and type.
  using ObjectId = std::pair<const void*, const std::type_info*>;

  // Constant for an ID to be used to identify a unique object not to be shared.
  constexpr static const ObjectId kUnique = {nullptr, nullptr};

  // Internal constructor accepting an object ID and a size.
  MemoryFootprint(ObjectId source, Memory self)
      : source_(source), self_(self) {}

  void PrintTo(std::ostream& out, std::string_view prefix) const;

  // A unique identifier for shared objects or null, if unique.
  ObjectId source_;

  // The memory usage of the described object.
  Memory self_;

  // A list of memory footprints of named sub-components.
  absl::flat_hash_map<std::string, MemoryFootprint> components_;
};

// Describes a concept for each type that provides memory footprint information.
template <typename I>
concept MemoryFootprintProvider = requires(const I a) {
  { a.GetMemoryFootprint() } -> std::same_as<MemoryFootprint>;
};

}  // namespace carmen
