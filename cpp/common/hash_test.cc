#include "common/hash.h"

#include <sstream>

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
  absl::flat_hash_map<std::string, std::string> res;
  res[""] =
      "0xe3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
  res["a"] =
      "0xca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb";
  res["abc"] =
      "0xba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
  return res;
}

TEST(Sha256Hash, TestKnownHashes) {
  for (auto [text, hash] : GetKnownHashes()) {
    Sha256Hasher hasher;
    hasher.Ingest(text);
    EXPECT_THAT(Print(hasher.GetHash()), StrEq(hash));
  }
}

TEST(Sha256Hash, HasherCanBeReset) {
  Sha256Hasher hasher;
  for (auto [text, hash] : GetKnownHashes()) {
    hasher.Reset();
    hasher.Ingest(text);
    EXPECT_THAT(Print(hasher.GetHash()), StrEq(hash));
  }
}

}  // namespace
}  // namespace carmen
