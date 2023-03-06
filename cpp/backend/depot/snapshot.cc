#include "backend/depot/snapshot.h"

#include <cstddef>
#include <span>
#include <vector>

#include "absl/status/statusor.h"

namespace carmen::backend::depot {

absl::StatusOr<DepotProof> DepotProof::FromBytes(
    std::span<const std::byte> data) {
  if (data.size() != sizeof(Hash)) {
    return absl::InvalidArgumentError(
        "Serialized DepotProof has invalid length");
  }
  Hash hash;
  hash.SetBytes(data.subspan(0, sizeof(Hash)));
  return DepotProof(hash);
}

std::vector<std::byte> DepotProof::ToBytes() const {
  std::vector<std::byte> res;
  res.reserve(sizeof(Hash));
  auto data = std::span<const std::byte>(hash);
  res.insert(res.end(), data.begin(), data.end());
  return res;
}

absl::StatusOr<DepotPart> DepotPart::FromBytes(
    std::span<const std::byte> data) {
  if (data.size() < sizeof(Proof)) {
    return absl::InvalidArgumentError(
        "Invalid encoding of store part, too few bytes.");
  }
  Proof proof;
  proof.hash.SetBytes(data.subspan(0, sizeof(Hash)));
  auto data_length = data.size() - sizeof(Hash);
  std::vector<std::byte> res;
  res.resize(data_length);
  std::memcpy(res.data(), data.subspan(sizeof(Hash)).data(), data_length);
  return DepotPart(proof, std::move(res));
}

std::vector<std::byte> DepotPart::ToBytes() const {
  std::vector<std::byte> res;
  res.reserve(sizeof(Hash) + data_.size());
  auto begin = reinterpret_cast<const std::byte*>(&proof_);
  res.insert(res.end(), begin, begin + sizeof(Proof));
  res.insert(res.end(), data_.begin(), data_.end());
  return res;
}

bool DepotPart::Verify() const {
  Hash have = GetSha256Hash(std::span(data_));
  return have == proof_.hash;
}

std::size_t DepotSnapshot::GetSize() const { return source_->GetSize(); }

DepotSnapshot::Proof DepotSnapshot::GetProof() const { return proof_; }

absl::StatusOr<DepotSnapshot::Proof> DepotSnapshot::GetProof(
    std::size_t part_number) const {
  return source_->GetProof(part_number);
}

absl::StatusOr<DepotSnapshot::Part> DepotSnapshot::GetPart(
    std::size_t part_number) const {
  return source_->GetPart(part_number);
}

absl::Status DepotSnapshot::VerifyProofs() const {
  // Collect all hashes of the pages.
  std::vector<Hash> hashes;
  hashes.reserve(GetSize());
  for (std::size_t i = 0; i < GetSize(); i++) {
    ASSIGN_OR_RETURN(auto proof, GetProof(i));
    hashes.push_back(proof.hash);
  }

  if (hashes.empty()) {
    return proof_.hash == Hash{}
               ? absl::OkStatus()
               : absl::InternalError("Proof chain is inconsistent.");
  }

  // Create a utility padding the hash vector to a length being a multiple of
  // the branching factor.
  auto pad_hashes = [&]() {
    if (hashes.size() % branching_factor_ != 0) {
      hashes.resize(hashes.size() +
                    (branching_factor_ - hashes.size() % branching_factor_));
    }
    assert(hashes.size() % branching_factor_ == 0);
  };

  while (hashes.size() > 1) {
    pad_hashes();
    for (std::size_t i = 0; i < hashes.size() / branching_factor_; i++) {
      hashes[i] = GetSha256Hash(std::as_bytes(
          std::span(hashes).subspan(i * branching_factor_, branching_factor_)));
    }
    hashes.resize(hashes.size() / branching_factor_);
  }

  return proof_.hash == hashes[0]
             ? absl::OkStatus()
             : absl::InternalError("Proof chain is inconsistent.");
}

}  // namespace carmen::backend::depot
