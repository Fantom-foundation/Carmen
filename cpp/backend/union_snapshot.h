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
constexpr void for_each_impl(Tuple&& t, Op&& op, std::index_sequence<Is...>) {
  (op.template operator()<std::integral_constant<std::size_t, Is>::value>(
       std::get<Is>(t)),
   ...);
}

template <class Tuple, class Op>
constexpr void for_each(Tuple&& t, Op&& op) {
  for_each_impl(std::forward<Tuple>(t), std::forward<Op>(op),
                std::make_index_sequence<
                    std::tuple_size_v<std::remove_cvref_t<Tuple>>>{});
}

}  // namespace internal

template <Snapshot... Snapshots>
class UnionSnapshot {
 public:
  using Proof = UnionProof<typename Snapshots::Proof...>;
  using Part = UnionPart<typename Snapshots::Part...>;

 private:
  class SubSnapshotDataSource;

  static absl::StatusOr<UnionSnapshot> Create(
      std::unique_ptr<std::vector<SubSnapshotDataSource>> sub_sources,
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
    return UnionSnapshot(std::move(sub_sources), *std::move(snapshots)...);
  }

 public:
  static absl::StatusOr<UnionSnapshot> Create(
      absl::StatusOr<Snapshots>... snapshots) {
    return Create(nullptr, std::move(snapshots)...);
  }

  static absl::StatusOr<UnionSnapshot> FromSource(
      const SnapshotDataSource& source) {
    ASSIGN_OR_RETURN(auto metadata, source.GetMetaData());
    std::span<const std::byte> data = metadata;

    // Read length of metadata of sub-snapshots.
    static constexpr auto kSizeOfLengthPrefix =
        sizeof(std::size_t) * sizeof...(Snapshots);
    if (data.size() < kSizeOfLengthPrefix) {
      return absl::InvalidArgumentError(
          "Invalid metadata encoding, to few bytes.");
    }

    // Split combined metadata into meta data of individual sub-snapshots.
    const std::size_t* meta_data_size =
        reinterpret_cast<const std::size_t*>(data.data());
    std::size_t offset = kSizeOfLengthPrefix;
    std::vector<std::vector<std::byte>> sub_metadata;
    for (std::size_t i = 0; i < sizeof...(Snapshots); i++) {
      std::size_t size = meta_data_size[i];
      if (data.size() < offset + size) {
        return absl::InvalidArgumentError(
            "Invalid metadata encoding, insufficient bytes for sub-metadata.");
      }
      auto current = std::span(data).subspan(offset, size);
      sub_metadata.push_back(
          std::vector<std::byte>(current.begin(), current.end()));
      offset += size;
    }

    // Create snapshot data for sub-snapshots.
    // TODO: keep sub-sources alive.
    auto sub_sources = std::make_unique<std::vector<SubSnapshotDataSource>>();
    sub_sources->reserve(sizeof...(Snapshots));

    offset = 0;
    std::tuple<absl::StatusOr<Snapshots>...> sub_snapshots;
    internal::for_each(
        sub_snapshots, [&]<std::size_t i, typename S>(S& snapshot) {
          sub_sources->push_back(SubSnapshotDataSource(
              std::move(sub_metadata[i]), offset, source));
          snapshot = S::value_type::FromSource(sub_sources->back());
          if (snapshot.ok()) {
            offset += snapshot->GetSize();
          }
        });

    // Create union snapshot from sub-snapshots.
    return std::apply(
        [&](auto... snapshots) {
          return Create(std::move(sub_sources), std::move(snapshots)...);
        },
        std::move(sub_snapshots));
  }

  const SnapshotDataSource& GetDataSource() const { return *raw_source_; }

  std::size_t GetSize() const {
    return std::apply(
        [](const auto&... snapshot) { return (0 + ... + snapshot.GetSize()); },
        *snapshots_);
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
        *snapshots_);
    if (want != proof_) {
      return absl::InternalError("Invalid proof for root of union snapshot.");
    }

    // Check the individual proof trees of the sub-snapshots.
    absl::Status result = absl::OkStatus();
    internal::for_each(*snapshots_, [&]<std::size_t>(auto& cur) {
      if (result.ok()) {
        result = cur.VerifyProofs();
      }
    });
    return result;
  }

  const std::tuple<Snapshots...>& GetSnapshots() const { return *snapshots_; }

 private:
  class RawSource : public SnapshotDataSource {
   public:
    RawSource(Proof proof, const std::tuple<Snapshots...>& snapshots)
        : proof_(proof), snapshots_(snapshots) {}

    absl::StatusOr<std::vector<std::byte>> GetMetaData() const override {
      // Collect the meta-data of the sub-snapshots.
      std::vector<std::vector<std::byte>> metadata;
      std::optional<absl::Status> error;
      internal::for_each(snapshots_, [&]<std::size_t>(const auto& snapshot) {
        if (error.has_value()) return;
        auto data = snapshot.GetDataSource().GetMetaData();
        if (!data.ok()) {
          error = data.status();
        } else {
          metadata.push_back(*data);
        }
      });
      if (error.has_value()) {
        return *error;
      }

      std::vector<std::byte> res;

      // Write the length of each meta data entry.
      auto offset = res.size();
      res.resize(res.size() + sizeof...(Snapshots) * sizeof(std::size_t));
      std::size_t* sizes = reinterpret_cast<std::size_t*>(res.data() + offset);
      for (std::size_t i = 0; i < metadata.size(); i++) {
        sizes[i] = metadata[i].size();
      }

      // Append the meta data.
      for (const auto& cur : metadata) {
        res.insert(res.end(), cur.begin(), cur.end());
      }

      return res;
    }

    absl::StatusOr<std::vector<std::byte>> GetProofData(
        std::size_t part_number) const override {
      return Extract<std::vector<std::byte>>(
          snapshots_, part_number,
          [&](const auto& snapshot,
              std::size_t offset) -> absl::StatusOr<std::vector<std::byte>> {
            ASSIGN_OR_RETURN(auto proof, snapshot.GetProof(offset));
            return proof.ToBytes();
          });
    }

    absl::StatusOr<std::vector<std::byte>> GetPartData(
        std::size_t part_number) const override {
      return Extract<std::vector<std::byte>>(
          snapshots_, part_number,
          [&](const auto& snapshot,
              std::size_t offset) -> absl::StatusOr<std::vector<std::byte>> {
            ASSIGN_OR_RETURN(auto part, snapshot.GetPart(offset));
            return part.ToBytes();
          });
    }

   private:
    const Proof proof_;
    const std::tuple<Snapshots...>& snapshots_;
  };

  class SubSnapshotDataSource : public SnapshotDataSource {
   public:
    SubSnapshotDataSource(std::vector<std::byte> metadata, std::size_t offset,
                          const SnapshotDataSource& original)
        : metadata_(std::move(metadata)), offset_(offset), source_(original) {}

    absl::StatusOr<std::vector<std::byte>> GetMetaData() const override {
      return metadata_;
    }

    absl::StatusOr<std::vector<std::byte>> GetProofData(
        std::size_t part_number) const override {
      return source_.GetProofData(offset_ + part_number);
    }

    absl::StatusOr<std::vector<std::byte>> GetPartData(
        std::size_t part_number) const override {
      return source_.GetPartData(offset_ + part_number);
    }

   private:
    std::vector<std::byte> metadata_;
    const std::size_t offset_;
    const SnapshotDataSource& source_;
  };

  UnionSnapshot(std::unique_ptr<std::vector<SubSnapshotDataSource>> sub_sources,
                Snapshots... snapshots)
      : proof_(Proof::Create(snapshots.GetProof()...)),
        snapshots_(std::make_unique<std::tuple<Snapshots...>>(
            std::move(snapshots)...)),
        raw_source_(std::make_unique<RawSource>(proof_, *snapshots_)),
        sub_sources_(std::move(sub_sources)) {}

  template <typename T, typename Op>
  static absl::StatusOr<T> Extract(const std::tuple<Snapshots...>& snapshots,
                                   std::size_t part_number, const Op& op) {
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
        snapshots);
  }

  template <typename T, typename Op>
  absl::StatusOr<T> Extract(std::size_t part_number, const Op& op) const {
    return Extract<T>(*snapshots_, part_number, op);
  }

  Proof proof_;

  std::unique_ptr<std::tuple<Snapshots...>> snapshots_;

  std::unique_ptr<RawSource> raw_source_;

  std::unique_ptr<std::vector<SubSnapshotDataSource>> sub_sources_;
};

}  // namespace carmen::backend
