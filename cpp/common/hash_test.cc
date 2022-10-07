#include "common/hash.h"

#include <sstream>
#include <type_traits>

#include "absl/container/flat_hash_map.h"
#include "common/type.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen {
namespace {

using ::testing::StrEq;

template <typename T>
std::string Print(const T& value) {
  std::stringstream out;
  out << value;
  return out.str();
}

absl::flat_hash_map<std::string, std::string> GetKnownHashes() {
  // The following hashes have been obtained from a third-party SHA256
  // implementation.
  absl::flat_hash_map<std::string, std::string> res;
  res[""] =
      "0xe3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
  res["a"] =
      "0xca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb";
  res["abc"] =
      "0xba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
  return res;
}

TEST(Sha256HashTest, TypeTraits) {
  EXPECT_TRUE(std::is_move_constructible_v<Sha256Hasher>);
}

TEST(Sha256HashTest, TestKnownHashes) {
  for (auto [text, hash] : GetKnownHashes()) {
    Sha256Hasher hasher;
    hasher.Ingest(text);
    EXPECT_THAT(Print(hasher.GetHash()), StrEq(hash));
  }
}

TEST(Sha256HashTest, HasherCanBeReset) {
  Sha256Hasher hasher;
  for (auto [text, hash] : GetKnownHashes()) {
    hasher.Reset();
    hasher.Ingest(text);
    EXPECT_THAT(Print(hasher.GetHash()), StrEq(hash));
  }
}

TEST(Sha256HashTest, SpansCanBeHashed) {
  Sha256Hasher hasher;

  std::array<std::byte, 1> data_a{std::byte{'a'}};
  hasher.Ingest(std::span<const std::byte>(data_a));
  EXPECT_THAT(Print(hasher.GetHash()), StrEq(GetKnownHashes()["a"]));

  hasher.Reset();
  std::array<std::byte, 3> data_b{std::byte{'a'}, std::byte{'b'},
                                  std::byte{'c'}};
  hasher.Ingest(std::span<const std::byte>(data_b));
  EXPECT_THAT(Print(hasher.GetHash()), StrEq(GetKnownHashes()["abc"]));
}

TEST(Sha256HashTest, ListOfTrivialObjectsCanBeIngested) {
  Sha256Hasher hasher;

  hasher.Ingest('a');
  EXPECT_THAT(Print(hasher.GetHash()), StrEq(GetKnownHashes()["a"]));

  hasher.Reset();
  hasher.Ingest('a', 'b', 'c');
  EXPECT_THAT(Print(hasher.GetHash()), StrEq(GetKnownHashes()["abc"]));
}

TEST(GetSha256Test, ComputesHashCorrectly) {
  auto hashes = GetKnownHashes();
  EXPECT_THAT(Print(GetSha256Hash()), StrEq(hashes[""]));
  EXPECT_THAT(Print(GetSha256Hash('a')), StrEq(hashes["a"]));
  EXPECT_THAT(Print(GetSha256Hash('a', 'b', 'c')), StrEq(hashes["abc"]));
}

TEST(Sha256HashTest, HashesCanBeHashed) {
  // The test passes if it compiles.
  Sha256Hasher hasher;
  hasher.Ingest(Hash{});
}

}  // namespace
}  // namespace carmen
