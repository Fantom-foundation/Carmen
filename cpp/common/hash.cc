/*
 * Copyright (c) 2024 Fantom Foundation
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE file and at fantom.foundation/bsl11.
 *
 * Change Date: 2028-4-16
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by the GNU Lesser General Public Licence v3.
 */

#include "common/hash.h"

#include <cstdint>
#include <memory>
#include <string_view>

#include "common/type.h"
#include "ethash/keccak.h"
#include "openssl/evp.h"
#include "openssl/sha.h"

namespace carmen {

namespace internal {

// Implements a wrapper over the OpenSSL Sha256 implementation.
class Sha256Impl {
 public:
  Sha256Impl() : _context(EVP_MD_CTX_new()) { Reset(); }
  ~Sha256Impl() { EVP_MD_CTX_destroy(_context); }

  void Reset() { EVP_DigestInit_ex(_context, EVP_sha256(), nullptr); }

  void Ingest(const std::byte* data, std::size_t length) {
    EVP_DigestUpdate(_context, data, length);
  }

  Hash GetHash() {
    Hash res;
    EVP_DigestFinal_ex(_context, reinterpret_cast<unsigned char*>(&res),
                       nullptr);
    return res;
  }

 private:
  EVP_MD_CTX* _context;
};

}  // namespace internal

Sha256Hasher::Sha256Hasher()
    : _impl(std::make_unique<internal::Sha256Impl>()) {}

Sha256Hasher::Sha256Hasher(Sha256Hasher&&) = default;

Sha256Hasher::~Sha256Hasher() {}

void Sha256Hasher::Reset() { _impl->Reset(); }

void Sha256Hasher::Ingest(const std::byte* data, std::size_t length) {
  _impl->Ingest(data, length);
}

void Sha256Hasher::Ingest(std::span<const std::byte> data) {
  _impl->Ingest(data.data(), data.size());
}

void Sha256Hasher::Ingest(const std::span<std::byte> data) {
  _impl->Ingest(data.data(), data.size());
}

void Sha256Hasher::Ingest(std::string_view str) {
  Ingest(reinterpret_cast<const std::byte*>(str.data()), str.size());
}

Hash Sha256Hasher::GetHash() const { return _impl->GetHash(); }

Hash GetSha256Hash(std::span<const std::byte> data) {
  Sha256Hasher hasher;
  return GetHash(hasher, data);
}

Hash GetKeccak256Hash(std::span<const std::byte> data) {
  static_assert(sizeof(Hash) == sizeof(ethash_hash256));
  ethash_hash256 res = ethash_keccak256(
      reinterpret_cast<const uint8_t*>(data.data()), data.size());
  return reinterpret_cast<Hash&>(res);
}

}  // namespace carmen
