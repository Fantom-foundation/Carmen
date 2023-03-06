#include "backend/index/snapshot.h"

#include <cstddef>
#include <span>
#include <vector>

#include "absl/status/statusor.h"
#include "common/type.h"

namespace carmen::backend::index {

absl::StatusOr<IndexProof> IndexProof::FromBytes(
    std::span<const std::byte> data) {
  if (data.size() != 2 * sizeof(Hash)) {
    return absl::InvalidArgumentError(
        "Serialized IndexProof has invalid length");
  }
  Hash begin;
  begin.SetBytes(data.subspan(0, sizeof(Hash)));
  Hash end;
  end.SetBytes(data.subspan(sizeof(Hash), sizeof(Hash)));
  return IndexProof(begin, end);
}

std::vector<std::byte> IndexProof::ToBytes() const {
  std::vector<std::byte> res;
  res.reserve(2 * sizeof(Hash));
  auto data = std::span<const std::byte>(begin);
  res.insert(res.end(), data.begin(), data.end());
  data = std::span<const std::byte>(end);
  res.insert(res.end(), data.begin(), data.end());
  return res;
}

}  // namespace carmen::backend::index
