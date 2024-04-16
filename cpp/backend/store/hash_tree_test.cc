/*
 * Copyright (c) 2024 Fantom Foundation
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at fantom.foundation/bsl11.
 *
 * Change Date: 2028-4-16
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by the GNU Lesser General Public Licence v3 
 */

#include "backend/store/hash_tree.h"

#include <filesystem>
#include <sstream>

#include "absl/status/statusor.h"
#include "common/file_util.h"
#include "common/status_test_util.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::backend::store {
namespace {

using ::testing::_;
using ::testing::IsOkAndHolds;
using ::testing::Return;
using ::testing::StatusIs;
using ::testing::StrEq;

class MockPageSource : public PageSource {
 public:
  MOCK_METHOD(absl::StatusOr<std::span<const std::byte>>, GetPageData,
              (PageId id), (override));
};

TEST(HashTreeTest, TypeTraits) {
  EXPECT_TRUE(std::is_move_constructible_v<HashTree>);
}

TEST(HashTreeTest, EmptyHashIsZero) {
  auto source = std::make_unique<MockPageSource>();
  HashTree tree(std::move(source));
  EXPECT_THAT(tree.GetHash(), IsOkAndHolds(Hash{}));
}

TEST(HashTreeTest, FetchingPageDataErrorIsHandled) {
  auto source = std::make_unique<MockPageSource>();
  auto& mock = *source.get();
  HashTree tree(std::move(source));

  EXPECT_CALL(mock, GetPageData(0))
      .WillOnce(Return(absl::InternalError("Error")));

  tree.MarkDirty(0);

  EXPECT_THAT(tree.GetHash(),
              StatusIs(absl::StatusCode::kInternal, StrEq("Error")));
}

TEST(HashTreeTest, HashOfSinglePageIsTheSameHash) {
  auto source = std::make_unique<MockPageSource>();
  HashTree tree(std::move(source));

  Hash hash{0x01, 0x02};
  tree.UpdateHash(0, hash);
  EXPECT_THAT(tree.GetHash(), IsOkAndHolds(hash));
}

TEST(HashTreeTest, HashesOfMultiplePagesAreAggregated) {
  auto source = std::make_unique<MockPageSource>();
  HashTree tree(std::move(source), /*branching_factor=*/4);

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
  EXPECT_THAT(tree.GetHash(), should);
}

TEST(HashTreeTest, AggregationMaySpanMultipleLevels) {
  auto source = std::make_unique<MockPageSource>();
  HashTree tree(std::move(source), /*branching_factor=*/2);

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
  EXPECT_THAT(tree.GetHash(), IsOkAndHolds(should));
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

  ASSERT_OK_AND_ASSIGN(auto should, tree.GetHash());
  EXPECT_THAT(tree.GetHash(), IsOkAndHolds(should));
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

  ASSERT_OK_AND_ASSIGN(auto hash_a, tree.GetHash());
  tree.UpdateHash(1, hash_2);
  ASSERT_OK_AND_ASSIGN(auto hash_b, tree.GetHash());
  tree.UpdateHash(1, hash_1);
  ASSERT_OK_AND_ASSIGN(auto hash_c, tree.GetHash());

  EXPECT_NE(hash_a, hash_b);
  EXPECT_EQ(hash_a, hash_c);
  EXPECT_NE(hash_b, hash_c);
}

TEST(HashTreeTest, DirtyPagesAreFetched) {
  auto source = std::make_unique<MockPageSource>();
  auto& mock = *source.get();
  HashTree tree(std::move(source));

  auto value = std::array{std::byte{0x01}, std::byte{0x02}};

  EXPECT_CALL(mock, GetPageData(0))
      .WillOnce(Return(absl::StatusOr<std::span<const std::byte>>(value)));
  tree.MarkDirty(0);
  ASSERT_OK(tree.GetHash());
}

TEST(HashTreeTest, MultipleDirtyPagesAreFetched) {
  auto source = std::make_unique<MockPageSource>();
  auto& mock = *source.get();
  HashTree tree(std::move(source));

  auto value = std::array{std::byte{0x01}, std::byte{0x02}};

  EXPECT_CALL(mock, GetPageData(0))
      .WillOnce(Return(absl::StatusOr<std::span<const std::byte>>(value)));
  EXPECT_CALL(mock, GetPageData(1))
      .WillOnce(Return(absl::StatusOr<std::span<const std::byte>>(value)));
  tree.MarkDirty(0);
  tree.MarkDirty(1);
  ASSERT_OK(tree.GetHash());
}

TEST(HashTreeTest, UpdateingHashesOfDirtyPagesResetsDirtyFlag) {
  auto source = std::make_unique<MockPageSource>();
  auto& mock = *source.get();
  HashTree tree(std::move(source));

  auto value = std::array{std::byte{0x01}, std::byte{0x02}};

  EXPECT_CALL(mock, GetPageData(0))
      .WillOnce(Return(absl::StatusOr<std::span<const std::byte>>(value)));
  EXPECT_CALL(mock, GetPageData(1)).Times(0);
  tree.MarkDirty(0);
  tree.MarkDirty(1);
  tree.UpdateHash(1, Hash{});
  ASSERT_OK(tree.GetHash());
}

TEST(HashTreeTest, RegistrationLeadsToTheIdentificationOfMissingPages) {
  auto source = std::make_unique<MockPageSource>();
  auto& mock = *source.get();
  HashTree tree(std::move(source));

  auto value = std::array{std::byte{0x01}, std::byte{0x02}};

  EXPECT_CALL(mock, GetPageData(0))
      .WillOnce(Return(absl::StatusOr<std::span<const std::byte>>(value)));
  EXPECT_CALL(mock, GetPageData(1))
      .WillOnce(Return(absl::StatusOr<std::span<const std::byte>>(value)));
  EXPECT_CALL(mock, GetPageData(2))
      .WillOnce(Return(absl::StatusOr<std::span<const std::byte>>(value)));
  EXPECT_CALL(mock, GetPageData(3))
      .WillOnce(Return(absl::StatusOr<std::span<const std::byte>>(value)));

  // After this, pages 0-3 are registered.
  tree.RegisterPage(3);

  // At this point, pages 0-3 should be 'dirty' and all are fetched.
  ASSERT_OK(tree.GetHash());
}

TEST(HashTreeTest, MissingPagesAreFetched) {
  auto source = std::make_unique<MockPageSource>();
  auto& mock = *source.get();
  HashTree tree(std::move(source));

  auto value = std::array{std::byte{0x01}, std::byte{0x02}};

  EXPECT_CALL(mock, GetPageData(0))
      .WillOnce(Return(absl::StatusOr<std::span<const std::byte>>(value)));
  EXPECT_CALL(mock, GetPageData(1))
      .WillOnce(Return(absl::StatusOr<std::span<const std::byte>>(value)));
  EXPECT_CALL(mock, GetPageData(2))
      .WillOnce(Return(absl::StatusOr<std::span<const std::byte>>(value)));
  EXPECT_CALL(mock, GetPageData(4))
      .WillOnce(Return(absl::StatusOr<std::span<const std::byte>>(value)));

  // After this, pages 0-5 are registered.
  tree.UpdateHash(5, Hash{});
  tree.UpdateHash(3, Hash{});

  // At this point, pages 0-2 and 4 should be 'dirty'.
  ASSERT_OK(tree.GetHash());
}

TEST(HashTreeTest, EmptyTreeCanBeSavedToFile) {
  auto source = std::make_unique<MockPageSource>();
  HashTree tree(std::move(source));

  TempFile file;
  std::filesystem::remove(file);
  ASSERT_OK(tree.SaveToFile(file));
  EXPECT_TRUE(std::filesystem::exists(file));
}

TEST(HashTreeTest, EmptyTreeCanBeSavedAndRestored) {
  TempFile file;
  std::filesystem::remove(file);
  Hash hash;
  {
    auto source = std::make_unique<MockPageSource>();
    HashTree tree(std::move(source));

    ASSERT_OK(tree.SaveToFile(file));
    EXPECT_TRUE(std::filesystem::exists(file));

    ASSERT_OK_AND_ASSIGN(hash, tree.GetHash());
  }

  {
    auto source = std::make_unique<MockPageSource>();
    HashTree tree(std::move(source));

    ASSERT_OK(tree.LoadFromFile(file));
    EXPECT_THAT(tree.GetHash(), IsOkAndHolds(hash));
  }
}

TEST(HashTreeTest, TreeWithPagesCanBeSavedAndRestored) {
  TempFile file;
  std::filesystem::remove(file);
  Hash hash;
  {
    auto source = std::make_unique<MockPageSource>();
    HashTree tree(std::move(source));

    tree.UpdateHash(0, Hash{0x01, 0x02});
    tree.UpdateHash(1, Hash{0x03, 0x04});
    tree.UpdateHash(2, Hash{0x05, 0x06});

    ASSERT_OK(tree.SaveToFile(file));
    EXPECT_TRUE(std::filesystem::exists(file));

    ASSERT_OK_AND_ASSIGN(hash, tree.GetHash());
  }

  {
    auto source = std::make_unique<MockPageSource>();
    HashTree tree(std::move(source));

    ASSERT_OK(tree.LoadFromFile(file));
    EXPECT_THAT(tree.GetHash(), IsOkAndHolds(hash));
  }
}

TEST(HashTreeTest, TreeWithMultipleLeveslCanBeSavedAndRestored) {
  TempFile file;
  std::filesystem::remove(file);
  Hash hash;
  {
    auto source = std::make_unique<MockPageSource>();
    HashTree tree(std::move(source), /*branching_factor=*/2);

    tree.UpdateHash(0, Hash{0x01, 0x02});
    tree.UpdateHash(1, Hash{0x03, 0x04});
    tree.UpdateHash(2, Hash{0x05, 0x06});

    ASSERT_OK(tree.SaveToFile(file));
    EXPECT_TRUE(std::filesystem::exists(file));

    ASSERT_OK_AND_ASSIGN(hash, tree.GetHash());
  }

  {
    auto source = std::make_unique<MockPageSource>();
    HashTree tree(std::move(source), /*branching_factor=*/2);

    ASSERT_OK(tree.LoadFromFile(file));
    EXPECT_THAT(tree.GetHash(), IsOkAndHolds(hash));
  }
}

TEST(HashTreeTest, EmptyTreeCanBeSavedToLevelDb) {
  auto source = std::make_unique<MockPageSource>();
  HashTree tree(std::move(source));

  TempDir dir;
  std::filesystem::remove(dir);
  ASSERT_OK(tree.SaveToLevelDb(dir));
  EXPECT_TRUE(std::filesystem::exists(dir));
}

TEST(HashTreeTest, EmptyTreeCanBeSavedAndRestoredFromLevelDb) {
  TempDir dir;
  std::filesystem::remove(dir);
  Hash hash;
  {
    auto source = std::make_unique<MockPageSource>();
    HashTree tree(std::move(source));

    ASSERT_OK(tree.SaveToLevelDb(dir));
    EXPECT_TRUE(std::filesystem::exists(dir));

    ASSERT_OK_AND_ASSIGN(hash, tree.GetHash());
  }

  {
    auto source = std::make_unique<MockPageSource>();
    HashTree tree(std::move(source));

    ASSERT_OK(tree.LoadFromLevelDb(dir));
    EXPECT_THAT(tree.GetHash(), IsOkAndHolds(hash));
  }
}

TEST(HashTreeTest, TreeWithPagesCanBeSavedAndRestoredFromLevelDb) {
  TempDir dir;
  std::filesystem::remove(dir);
  Hash hash;
  {
    auto source = std::make_unique<MockPageSource>();
    HashTree tree(std::move(source));

    tree.UpdateHash(0, Hash{0x01, 0x02});
    tree.UpdateHash(1, Hash{0x03, 0x04});
    tree.UpdateHash(2, Hash{0x05, 0x06});

    ASSERT_OK(tree.SaveToLevelDb(dir));
    EXPECT_TRUE(std::filesystem::exists(dir));

    ASSERT_OK_AND_ASSIGN(hash, tree.GetHash());
  }

  {
    auto source = std::make_unique<MockPageSource>();
    HashTree tree(std::move(source));

    ASSERT_OK(tree.LoadFromLevelDb(dir));
    EXPECT_THAT(tree.GetHash(), IsOkAndHolds(hash));
  }
}

TEST(HashTreeTest, TreeWithMultipleLeveslCanBeSavedAndRestoredFromLevelDb) {
  TempDir dir;
  std::filesystem::remove(dir);
  Hash hash;
  {
    auto source = std::make_unique<MockPageSource>();
    HashTree tree(std::move(source), /*branching_factor=*/2);

    tree.UpdateHash(0, Hash{0x01, 0x02});
    tree.UpdateHash(1, Hash{0x03, 0x04});
    tree.UpdateHash(2, Hash{0x05, 0x06});

    ASSERT_OK(tree.SaveToLevelDb(dir));
    EXPECT_TRUE(std::filesystem::exists(dir));

    ASSERT_OK_AND_ASSIGN(hash, tree.GetHash());
  }

  {
    auto source = std::make_unique<MockPageSource>();
    HashTree tree(std::move(source), /*branching_factor=*/2);

    ASSERT_OK(tree.LoadFromLevelDb(dir));
    EXPECT_THAT(tree.GetHash(), IsOkAndHolds(hash));
  }
}
}  // namespace
}  // namespace carmen::backend::store
