#include "backend/index/memory/index.h"

#include <type_traits>

#include "common/hash.h"
#include "common/type.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::backend::index {
namespace {

using ::testing::Optional;

using TestIndex = InMemoryIndex<int, int>;

TEST(InMemoryIndexTest, TypeProperties) {
  EXPECT_TRUE(std::is_default_constructible_v<TestIndex>);
  EXPECT_TRUE(std::is_move_constructible_v<TestIndex>);
}

TEST(InMemoryIndexTest, IdentifiersAreAssignedInorder) {
  TestIndex index;
  EXPECT_EQ(0, index.GetOrAdd(1));
  EXPECT_EQ(1, index.GetOrAdd(2));
  EXPECT_EQ(2, index.GetOrAdd(3));
}

TEST(InMemoryIndexTest, SameKeyLeadsToSameIdentifier) {
  TestIndex index;
  EXPECT_EQ(0, index.GetOrAdd(1));
  EXPECT_EQ(1, index.GetOrAdd(2));
  EXPECT_EQ(0, index.GetOrAdd(1));
  EXPECT_EQ(1, index.GetOrAdd(2));
}

TEST(InMemoryIndexTest, ContainsIdentifiesIndexedElements) {
  TestIndex index;
  EXPECT_FALSE(index.Contains(1));
  EXPECT_FALSE(index.Contains(2));
  EXPECT_FALSE(index.Contains(3));

  EXPECT_EQ(0, index.GetOrAdd(1));
  EXPECT_TRUE(index.Contains(1));
  EXPECT_FALSE(index.Contains(2));
  EXPECT_FALSE(index.Contains(3));

  EXPECT_EQ(1, index.GetOrAdd(2));
  EXPECT_TRUE(index.Contains(1));
  EXPECT_TRUE(index.Contains(2));
  EXPECT_FALSE(index.Contains(3));
}

TEST(InMemoryIndexTest, GetRetrievesPresentKeys) {
  TestIndex index;
  EXPECT_EQ(index.Get(1), std::nullopt);
  EXPECT_EQ(index.Get(2), std::nullopt);
  auto id1 = index.GetOrAdd(1);
  EXPECT_THAT(index.Get(1), Optional(id1));
  EXPECT_EQ(index.Get(2), std::nullopt);
  auto id2 = index.GetOrAdd(2);
  EXPECT_THAT(index.Get(1), Optional(id1));
  EXPECT_THAT(index.Get(2), Optional(id2));
}

TEST(InMemoryIndexTest, EmptyIndexHasHashEqualsZero) {
  TestIndex index;
  EXPECT_EQ(Hash{}, index.GetHash());
}

TEST(InMemoryIndexTest, IndexHashIsEqualToInsertionOrder) {
  Hash hash;
  TestIndex index;
  EXPECT_EQ(hash, index.GetHash());
  index.GetOrAdd(12);
  hash = GetSha256Hash(hash, 12);
  EXPECT_EQ(hash, index.GetHash());
  index.GetOrAdd(14);
  hash = GetSha256Hash(hash, 14);
  EXPECT_EQ(hash, index.GetHash());
  index.GetOrAdd(16);
  hash = GetSha256Hash(hash, 16);
  EXPECT_EQ(hash, index.GetHash());
}

}  // namespace
}  // namespace carmen::backend::index
