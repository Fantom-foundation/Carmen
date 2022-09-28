#include "backend/store/file/hash_tree.h"

#include <filesystem>
#include <sstream>

#include "common/file_util.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::backend::store {
namespace {

using ::testing::_;

class MockPageSource : public PageSource {
 public:
  MOCK_METHOD(std::span<const std::byte>, GetPageData, (PageId id), (override));
};

TEST(HashTreeTest, EmptyHashIsZero) {
  auto source = std::make_unique<MockPageSource>();
  HashTree tree(std::move(source));
  EXPECT_EQ(Hash{}, tree.GetHash());
}

TEST(HashTreeTest, HashOfSinglePageIsTheSameHash) {
  auto source = std::make_unique<MockPageSource>();
  HashTree tree(std::move(source));

  Hash hash{0x01, 0x02};
  tree.UpdateHash(0, hash);
  EXPECT_EQ(hash, tree.GetHash());
}

TEST(HashTreeTest, HashesOfMultiplePagesAreAggregated) {
  auto source = std::make_unique<MockPageSource>();
  HashTree tree(std::move(source), /*branch_factor=*/4);

  // The hashes for the first 3 pages is fixed.
  Hash hash_0{0x01, 0x02};
  tree.UpdateHash(0, hash_0);
  Hash hash_1{0x03, 0x04};
  tree.UpdateHash(1, hash_1);
  Hash hash_2{0x05, 0x06};
  tree.UpdateHash(2, hash_2);

  // The total hash should be the hash of the concatenation of the hashes of the
  // first level and a padded zero hash.
  auto should = GetSha256Hash(hash_0, hash_1, hash_2, Hash{});
  EXPECT_EQ(should, tree.GetHash());
}

TEST(HashTreeTest, AggregationMaySpanMultipleLevels) {
  auto source = std::make_unique<MockPageSource>();
  HashTree tree(std::move(source), /*branch_factor=*/2);

  // The hashes for the first 3 pages is fixed.
  Hash hash_0{0x01, 0x02};
  tree.UpdateHash(0, hash_0);
  Hash hash_1{0x03, 0x04};
  tree.UpdateHash(1, hash_1);
  Hash hash_2{0x05, 0x06};
  tree.UpdateHash(2, hash_2);

  // The total hash should be the two-layer reduction of the hashes.
  auto should = GetSha256Hash(GetSha256Hash(hash_0, hash_1),
                              GetSha256Hash(hash_2, Hash{}));
  EXPECT_EQ(should, tree.GetHash());
}

TEST(HashTreeTest, HashIsTheSameIfQueriedMultipleTimesWithoutChanges) {
  auto source = std::make_unique<MockPageSource>();
  HashTree tree(std::move(source));

  // The hashes for the first 3 pages is fixed.
  Hash hash_0{0x01, 0x02};
  tree.UpdateHash(0, hash_0);
  Hash hash_1{0x03, 0x04};
  tree.UpdateHash(1, hash_1);
  Hash hash_2{0x05, 0x06};
  tree.UpdateHash(2, hash_2);

  auto should = tree.GetHash();
  EXPECT_EQ(should, tree.GetHash());
}

TEST(HashTreeTest, HashIsDifferentIfPageHashesAreDifferentAndTheSameIfTheSame) {
  auto source = std::make_unique<MockPageSource>();
  HashTree tree(std::move(source));

  // The hashes for the first 3 pages is fixed.
  Hash hash_0{0x01, 0x02};
  tree.UpdateHash(0, hash_0);
  Hash hash_1{0x03, 0x04};
  tree.UpdateHash(1, hash_1);
  Hash hash_2{0x05, 0x06};
  tree.UpdateHash(2, hash_2);

  auto hash_a = tree.GetHash();
  tree.UpdateHash(1, hash_2);
  auto hash_b = tree.GetHash();
  tree.UpdateHash(1, hash_1);
  auto hash_c = tree.GetHash();

  EXPECT_NE(hash_a, hash_b);
  EXPECT_EQ(hash_a, hash_c);
  EXPECT_NE(hash_b, hash_c);
}

TEST(HashTreeTest, DirtyPagesAreFetched) {
  auto source = std::make_unique<MockPageSource>();
  auto& mock = *source.get();
  HashTree tree(std::move(source));

  EXPECT_CALL(mock, GetPageData(0));
  tree.MarkDirty(0);
  tree.GetHash();
}

TEST(HashTreeTest, MultipleDirtyPagesAreFetched) {
  auto source = std::make_unique<MockPageSource>();
  auto& mock = *source.get();
  HashTree tree(std::move(source));

  EXPECT_CALL(mock, GetPageData(0));
  EXPECT_CALL(mock, GetPageData(1));
  tree.MarkDirty(0);
  tree.MarkDirty(1);
  tree.GetHash();
}

TEST(HashTreeTest, UpdateingHashesOfDirtyPagesResetsDirtyFlag) {
  auto source = std::make_unique<MockPageSource>();
  auto& mock = *source.get();
  HashTree tree(std::move(source));

  EXPECT_CALL(mock, GetPageData(0));
  EXPECT_CALL(mock, GetPageData(1)).Times(0);
  tree.MarkDirty(0);
  tree.MarkDirty(1);
  tree.UpdateHash(1, Hash{});
  tree.GetHash();
}

TEST(HashTreeTest, MissingPagesAreFetched) {
  auto source = std::make_unique<MockPageSource>();
  auto& mock = *source.get();
  HashTree tree(std::move(source));

  EXPECT_CALL(mock, GetPageData(0));
  EXPECT_CALL(mock, GetPageData(1));
  EXPECT_CALL(mock, GetPageData(2));
  EXPECT_CALL(mock, GetPageData(4));

  // After this, pages 0-5 are registered.
  tree.UpdateHash(5, Hash{});
  tree.UpdateHash(3, Hash{});

  // At this point, pages 0-2 and 4 should be 'dirty'.
  tree.GetHash();
}

}  // namespace
}  // namespace carmen::backend::store
