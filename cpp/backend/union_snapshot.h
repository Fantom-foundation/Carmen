#pragma once

#include <cstddef>
#include <span>
#include <tuple>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "backend/snapshot.h"
#include "common/hash.h"
#include "common/status_util.h"
#include "common/variant_util.h"

namespace carmen::backend {

namespace internal {

// The type of proof used to certify the proper composition of sub-snapshots in
// snapshot unions.
class UnionRootProof {
 public:
  UnionRootProof(const Hash& hash = Hash{}) : hash_(hash) {}

  static absl::StatusOr<UnionRootProof> FromBytes(
      std::span<const std::byte> data) {
    if (data.size() != sizeof(Hash)) {
      return absl::InvalidArgumentError(
          "Serialized UnionRootProof has invalid length");
    }
    Hash hash;
    hash.SetBytes(data);
    return UnionRootProof(hash);
  }

  std::vector<std::byte> ToBytes() const {
    std::span<const std::byte> data(hash_);
    return {data.begin(), data.end()};
  }

  const Hash& GetHash() const { return hash_; }

  bool operator==(const UnionRootProof&) const = default;

 private:
  Hash hash_;
};

static_assert(Proof<UnionRootProof>);

template <typename... Types>
struct type_list {};

template <typename T>
struct to_type_list;

template <typename... Ts>
struct to_type_list<std::variant<Ts...>> {
  using type = type_list<Ts...>;
};

template <typename T>
using to_type_list_t = typename to_type_list<T>::type;

template <typename Op, typename... Type>
void for_each_type(type_list<Type...>, const Op& op) {
  (op.template operator()<Type>(), ...);
}

}  // namespace internal

template <Proof... Proofs>
class UnionProof {
 public:
  UnionProof() = default;

  UnionProof(Hash hash) : proof_(internal::UnionRootProof(std::move(hash))) {}

  template <Proof P>
  UnionProof(P proof) : proof_(std::move(proof)) {}

  static UnionProof Create(Proofs... proofs) {
    return UnionProof(GetSha256Hash(proofs.ToBytes()...));
  }

  bool operator==(const UnionProof&) const = default;

  static absl::StatusOr<UnionProof> FromBytes(std::span<const std::byte> data) {
    if (data.empty()) {
      return absl::InvalidArgumentError(
          "Serialized UnionProof has invalid length");
    }
    auto type = static_cast<std::uint8_t>(data[0]);
    auto rest = data.subspan(1);

    // Use proof specific parser for specialized proofs.
    std::uint8_t i = 0;
    absl::StatusOr<UnionProof> result =
        absl::InvalidArgumentError("Unsupported union proof type.");
    internal::for_each_type(internal::to_type_list_t<
                                Variant<internal::UnionRootProof, Proofs...>>{},
                            [&]<typename Proof>() {
                              if (type == i++) {
                                result = Proof::FromBytes(rest);
                              }
                            });
    return result;
  }

  std::vector<std::byte> ToBytes() const {
    static_assert(std::variant_size_v<decltype(proof_)> < 256,
                  "Only supporting up to 256 different proof types.");
    auto proof_bytes =
        std::visit([](const auto& cur) { return cur.ToBytes(); }, proof_);
    std::vector<std::byte> res;
    // 1 byte for the type of proof.
    res.reserve(1 + proof_bytes.size());
    std::uint8_t index = proof_.index();
    res.push_back(std::byte(index));
    res.insert(res.end(), proof_bytes.begin(), proof_bytes.end());
    return res;
  }

 private:
  // The actual proof may by any of the proofs of the union snapshot or a root
  // proof, being the type of proof for the overall union snapshot.
  Variant<internal::UnionRootProof, Proofs...> proof_;
};

template <Part... Parts>
class UnionPart {
 public:
  using Proof = UnionProof<typename Parts::Proof...>;

  template <Part Part>
  UnionPart(Part part) : part_(std::move(part)) {}

  static absl::StatusOr<UnionPart> FromBytes(std::span<const std::byte> data) {
    if (data.empty()) {
      return absl::InvalidArgumentError(
          "Serialized UnionPart has invalid length");
    }
    auto type = static_cast<std::uint8_t>(data[0]);
    auto rest = data.subspan(1);

    // Use part specific parser for specialized parts.
    std::uint8_t i = 0;
    absl::StatusOr<UnionPart> result =
        absl::InvalidArgumentError("Unsupported union part type.");
    internal::for_each_type(internal::to_type_list_t<Variant<Parts...>>{},
                            [&]<typename Part>() {
                              if (type == i++) {
                                result = Part::FromBytes(rest);
                              }
                            });
    return result;
  }

  std::vector<std::byte> ToBytes() const {
    static_assert(std::variant_size_v<decltype(part_)> < 256,
                  "Only supporting up to 256 different part types.");
    auto part_bytes =
        std::visit([](const auto& cur) { return cur.ToBytes(); }, part_);
    std::vector<std::byte> res;
    // 1 byte for the type of part.
    res.reserve(1 + part_bytes.size());
    std::uint8_t index = part_.index();
    res.push_back(std::byte(index));
    res.insert(res.end(), part_bytes.begin(), part_bytes.end());
    return res;
  }

  Proof GetProof() const {
    return std::visit([&](const auto& cur) -> Proof { return cur.GetProof(); },
                      part_);
  }

  bool Verify() const {
    return std::visit([&](const auto& cur) { return cur.Verify(); }, part_);
  }

 private:
  Variant<Parts...> part_;
};

namespace internal {

template <class Tuple, class Op, std::size_t... Is>
constexpr void for_each_impl(const Tuple& t, Op&& op,
                             std::index_sequence<Is...>) {
  (op(std::integral_constant<std::size_t, Is>{}, std::get<Is>(t)), ...);
}

template <class... T, class Op>
constexpr void for_each(const std::tuple<T...>& t, Op&& op) {
  for_each_impl(t, std::forward<Op>(op),
                std::make_index_sequence<sizeof...(T)>{});
}

}  // namespace internal

template <Snapshot... Snapshots>
class UnionSnapshot {
 public:
  using Proof = UnionProof<typename Snapshots::Proof...>;
  using Part = UnionPart<typename Snapshots::Part...>;

  static absl::StatusOr<UnionSnapshot> Create(
      absl::StatusOr<Snapshots>... snapshots) {
    // Check provided snapshots for errors ...
    absl::Status err = absl::OkStatus();
    auto check = [&](const auto& cur) {
      if (err.ok() && !cur.ok()) {
        err = cur.status();
      }
    };
    (check(snapshots), ...);
    RETURN_IF_ERROR(err);
    // Combine snapshots into a single snapshot.
    return UnionSnapshot(*std::move(snapshots)...);
  }

  std::size_t GetSize() const {
    return std::apply(
        [](const auto&... snapshot) { return (0 + ... + snapshot.GetSize()); },
        snapshots_);
  }

  absl::StatusOr<Part> GetPart(std::size_t part_number) const {
    return Extract<Part>(part_number,
                         [](const auto& snapshot, std::size_t offset) {
                           return snapshot.GetPart(offset);
                         });
  }

  Proof GetProof() const { return proof_; }

  absl::StatusOr<Proof> GetProof(std::size_t part_number) const {
    return Extract<Proof>(part_number,
                          [](const auto& snapshot, std::size_t offset) {
                            return snapshot.GetProof(offset);
                          });
  }

  absl::Status VerifyProofs() const {
    // Check root hash first.
    auto want = std::apply(
        [](const auto&... snapshot) {
          return Proof::Create(snapshot.GetProof()...);
        },
        snapshots_);
    if (want != proof_) {
      return absl::InternalError("Invalid proof for root of union snapshot.");
    }

    // Check the individual proof trees of the sub-snapshots.
    absl::Status result = absl::OkStatus();
    internal::for_each(snapshots_, [&](std::size_t, auto& cur) {
      if (result.ok()) {
        result = cur.VerifyProofs();
      }
    });
    return result;
  }

  const std::tuple<Snapshots...>& GetSnapshots() const { return snapshots_; }

 private:
  UnionSnapshot(Snapshots... snapshots)
      : proof_(Proof::Create(snapshots.GetProof()...)),
        snapshots_(std::move(snapshots)...) {}

  template <typename T, typename Op>
  absl::StatusOr<T> Extract(std::size_t part_number, const Op& op) const {
    return std::apply(
        [&](const auto&... snapshot) {
          absl::StatusOr<T> res = absl::InvalidArgumentError("no such part");
          bool done = false;
          auto get = [&](const auto& snapshot) {
            if (done) return;
            auto current_size = snapshot.GetSize();
            if (part_number < current_size) {
              res = op(snapshot, part_number);
              done = true;
            } else {
              part_number -= current_size;
            }
          };
          (get(snapshot), ...);
          return res;
        },
        snapshots_);
  }

  Proof proof_;
  std::tuple<Snapshots...> snapshots_;
};

}  // namespace carmen::backend
