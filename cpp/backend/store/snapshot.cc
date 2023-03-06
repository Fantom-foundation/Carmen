#include "backend/store/snapshot.h"

#include <cstddef>
#include <span>
#include <vector>

#include "absl/status/statusor.h"
#include "common/type.h"

namespace carmen::backend::store {

absl::StatusOr<StoreProof> StoreProof::FromBytes(
    std::span<const std::byte> data) {
  if (data.size() != sizeof(Hash)) {
    return absl::InvalidArgumentError(
        "Serialized StoreProof has invalid length");
  }
  Hash hash;
  hash.SetBytes(data.subspan(0, sizeof(Hash)));
  return StoreProof(hash);
}

std::vector<std::byte> StoreProof::ToBytes() const {
  std::vector<std::byte> res;
  res.reserve(sizeof(Hash));
  auto data = std::span<const std::byte>(hash);
  res.insert(res.end(), data.begin(), data.end());
  return res;
}

}  // namespace carmen::backend::store
