#pragma once

#include <cstdint>
#include <memory>
#include <span>
#include <string_view>

#include "common/type.h"

namespace carmen {

namespace internal {
class Sha256Impl;
}

// A utility class to compute the SHA256 hash of data.
//
// To hash data, create an instance, feed in data using the class's Ingest(..)
// functions, and consume the final hash using GetHash(). Once a hash is
// consumed, no more input may be added.
//
// Instances can be reused for multiple hash computation by reseting them
// between hashing operations. This is more efficient than recreating a new
// instance for each step.
class Sha256Hasher {
 public:
  Sha256Hasher();
  Sha256Hasher(Sha256Hasher&&);
  ~Sha256Hasher();

  // Adds the given byte array to the sequence of bytes to hashed.
  void Ingest(const std::byte* data, std::size_t length);

  // Same as above, but using a span to represent a sequence of bytes.
  void Ingest(const std::span<const std::byte> span);

  // Same as above, but for a span of mutable bytes.
  void Ingest(const std::span<std::byte> span);

  // A convenience variant of the function above, supporting the hashing of
  // strings through a single parameter.
  void Ingest(std::string_view str);

  // A no-op serving as the base case for ingesting lists of trivial types.
  void Ingest() {}

  // A convenience variant of the fuction above, supporting the hashing of
  // all trivial types.
  template <Trivial T>
  void Ingest(const T& value) {
    Ingest(reinterpret_cast<const std::byte*>(&value), sizeof(T));
  }

  // An extension of the fuction above, supporting the ingestion of a list
  // of trival objects.
  template <Trivial First, Trivial... Rest>
  void Ingest(const First& first, const Rest&... rest) {
    Ingest(first);
    Ingest(rest...);
  }

  // Finalises the hashing and consumes the resulting hash.
  Hash GetHash() const;

  // Resets this instance by forgetting the data consumed so far, allowing
  // instances to be reused for multiple hashes.
  void Reset();

 private:
  // The actual implementation of the hasher is hidden behind a interanl
  // data type (Pimpl-pattern) to avoid including headers referencing
  // implementation details into this header file, and those avoiding their
  // import in other files.
  std::unique_ptr<internal::Sha256Impl> _impl;
};

// A utility fuiction to hash a list of trivial elements using the given hasher
// instance. The state of the handed in hasher is reset before ingesting the
// provided list of elements.
template <Trivial... Elements>
Hash GetHash(Sha256Hasher& hasher, const Elements&... elements) {
  hasher.Reset();
  hasher.Ingest(elements...);
  return hasher.GetHash();
}

// A utility function to compute the SHA256 hash of a list of trivial elements.
// It iternally creats a Sha256Hasher instance for computing the hash. If
// multiple hashes are to be computed, consider creating such an instance in the
// caller scope and reusing the instance for all invocations.
template <Trivial... Elements>
Hash GetSha256Hash(const Elements&... elements) {
  Sha256Hasher hasher;
  return GetHash(hasher, elements...);
}

// A utility function to compute the Keccak256 hash of the given data blob.
Hash GetKeccak256Hash(std::span<const std::byte> data);

}  // namespace carmen
