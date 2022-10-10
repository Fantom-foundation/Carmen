#include "backend/index/leveldb/index.h"

#include "absl/status/statusor.h"
#include "common/file_util.h"
#include "common/hash.h"
#include "common/type.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::backend::index {
namespace {

using ::testing::StrEq;

LevelDBKeySpace<int, int> GetTestIndex(const TempDir& dir = TempDir()) {
  return LevelDBIndex(dir.GetPath().string()).Balance<int, int>();
}

TEST(LevelDBIndexTest, ConvertToLevelDBKey) {
  auto key = internal::ToDBKey(internal::KeySpace::kValue, 1);
  std::string val(1, static_cast<char>(internal::KeySpace::kValue));
  EXPECT_THAT(key, StrEq(val + "1"));
}

TEST(LevelDBIndexTest, ConvertAndParseLevelDBValue) {
  std::uint8_t input = 69;
  auto value = internal::ToDBValue(input);
  EXPECT_EQ(input, internal::ParseDBResult<std::uint8_t>(value));
}

TEST(LevelDBIndexTest, IdentifiersAreAssignedInorder) {
  auto index = GetTestIndex();
  EXPECT_EQ(0, index.GetOrAdd(1).value());
  EXPECT_EQ(1, index.GetOrAdd(2).value());
  EXPECT_EQ(2, index.GetOrAdd(3).value());
}

TEST(LevelDBIndexTest, SameKeyLeadsToSameIdentifier) {
  auto index = GetTestIndex();
  EXPECT_EQ(0, index.GetOrAdd(1).value());
  EXPECT_EQ(1, index.GetOrAdd(2).value());
  EXPECT_EQ(0, index.GetOrAdd(1).value());
  EXPECT_EQ(1, index.GetOrAdd(2).value());
}

TEST(LevelDBIndexTest, ContainsIdentifiesIndexedElements) {
  auto index = GetTestIndex();
  EXPECT_FALSE(index.Contains(1));
  EXPECT_FALSE(index.Contains(2));
  EXPECT_FALSE(index.Contains(3));

  EXPECT_EQ(0, index.GetOrAdd(1).value());
  EXPECT_TRUE(index.Contains(1));
  EXPECT_FALSE(index.Contains(2));
  EXPECT_FALSE(index.Contains(3));

  EXPECT_EQ(1, index.GetOrAdd(2).value());
  EXPECT_TRUE(index.Contains(1));
  EXPECT_TRUE(index.Contains(2));
  EXPECT_FALSE(index.Contains(3));
}

TEST(LevelDBIndexTest, GetRetrievesPresentKeys) {
  auto index = GetTestIndex();
  EXPECT_EQ(index.Get(1).status().code(), absl::StatusCode::kNotFound);
  EXPECT_EQ(index.Get(2).status().code(), absl::StatusCode::kNotFound);
  auto id1 = index.GetOrAdd(1);
  EXPECT_THAT(index.Get(1).value(), id1.value());
  EXPECT_EQ(index.Get(2).status().code(), absl::StatusCode::kNotFound);
  auto id2 = index.GetOrAdd(2);
  EXPECT_THAT(index.Get(1).value(), id1.value());
  EXPECT_THAT(index.Get(2).value(), id2.value());
}

TEST(LevelDBIndexTest, EmptyIndexHasHashEqualsZero) {
  auto index = GetTestIndex();
  EXPECT_EQ(Hash{}, index.GetHash().value());
}

TEST(LevelDBIndexTest, IndexHashIsEqualToInsertionOrder) {
  Hash hash;
  auto index = GetTestIndex();
  EXPECT_EQ(hash, index.GetHash().value());
  index.GetOrAdd(12);
  hash = GetSha256Hash(hash, 12);
  EXPECT_EQ(hash, index.GetHash().value());
  index.GetOrAdd(14);
  hash = GetSha256Hash(hash, 14);
  EXPECT_EQ(hash, index.GetHash().value());
  index.GetOrAdd(16);
  hash = GetSha256Hash(hash, 16);
  EXPECT_EQ(hash, index.GetHash().value());
}

TEST(LevelDBIndexTest, IndexIsPersistent) {
  TempDir dir = TempDir();
  absl::StatusOr<int> id1;

  // Insert value in a separate block to ensure that the index is closed.
  {
    auto index = GetTestIndex(dir);
    EXPECT_THAT(index.Get(1).status().code(), absl::StatusCode::kNotFound);
    id1 = index.GetOrAdd(1);
    EXPECT_THAT(index.Get(1).value(), id1.value());
  }

  // Reopen index and check that the value is still present.
  {
    auto index = GetTestIndex(dir);
    EXPECT_THAT(index.Get(1).value(), id1.value());
  }
}

}  // namespace
}  // namespace carmen::backend::index
