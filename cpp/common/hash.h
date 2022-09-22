#pragma once

#include <cstdint>
#include <memory>
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
  ~Sha256Hasher();

  // Adds the given byte array to the sequence of bytes to hashed.
  void Ingest(const std::byte* data, std::size_t length);

  // A convenience variant of the function above, supporting the hashing of
  // strings through a single parameter.
  void Ingest(std::string_view str);

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

}  // namespace carmen
