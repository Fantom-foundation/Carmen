#include "backend/index/memory/index.h"

#include <type_traits>

#include "backend/common/file.h"
#include "backend/index/file/index.h"
#include "backend/index/index_handler.h"
#include "backend/index/leveldb/multi_db/index.h"
#include "backend/index/leveldb/single_db/index.h"
#include "backend/index/memory/linear_hash_index.h"
#include "common/status_test_util.h"
#include "common/test_util.h"
#include "common/type.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::backend::index {
namespace {

using ::testing::_;
using ::testing::IsOk;
using ::testing::IsOkAndHolds;
using ::testing::Optional;
using ::testing::StatusIs;
using ::testing::StrEq;

// Implements a generic test suite for index implementations checking basic
// properties like GetOrAdd, contains, and hashing functionality.
template <Index I>
class IndexTest : public testing::Test {};

TYPED_TEST_SUITE_P(IndexTest);

TYPED_TEST_P(IndexTest, TypeProperties) {
  ASSERT_OK_AND_ASSIGN(auto wrapper, IndexHandler<TypeParam>::Create());
  auto& index = wrapper.GetIndex();
  EXPECT_TRUE(std::is_move_constructible_v<decltype(index)>);
}

TYPED_TEST_P(IndexTest, IdentifiersAreAssignedInorder) {
  ASSERT_OK_AND_ASSIGN(auto wrapper, IndexHandler<TypeParam>::Create());
  auto& index = wrapper.GetIndex();
  EXPECT_THAT(index.GetOrAdd(1), IsOkAndHolds(std::pair(0, true)));
  EXPECT_THAT(index.GetOrAdd(2), IsOkAndHolds(std::pair(1, true)));
  EXPECT_THAT(index.GetOrAdd(3), IsOkAndHolds(std::pair(2, true)));
}

TYPED_TEST_P(IndexTest, SameKeyLeadsToSameIdentifier) {
  ASSERT_OK_AND_ASSIGN(auto wrapper, IndexHandler<TypeParam>::Create());
  auto& index = wrapper.GetIndex();
  EXPECT_THAT(index.GetOrAdd(1), IsOkAndHolds(std::pair(0, true)));
  EXPECT_THAT(index.GetOrAdd(2), IsOkAndHolds(std::pair(1, true)));
  EXPECT_THAT(index.GetOrAdd(1), IsOkAndHolds(std::pair(0, false)));
  EXPECT_THAT(index.GetOrAdd(2), IsOkAndHolds(std::pair(1, false)));
}

TYPED_TEST_P(IndexTest, ContainsIdentifiesIndexedElements) {
  ASSERT_OK_AND_ASSIGN(auto wrapper, IndexHandler<TypeParam>::Create());
  auto& index = wrapper.GetIndex();

  EXPECT_THAT(index.Get(1), StatusIs(absl::StatusCode::kNotFound, _));
  EXPECT_THAT(index.Get(2), StatusIs(absl::StatusCode::kNotFound, _));
  EXPECT_THAT(index.Get(3), StatusIs(absl::StatusCode::kNotFound, _));

  EXPECT_THAT(index.GetOrAdd(1), IsOkAndHolds(std::pair(0, true)));
  EXPECT_THAT(index.Get(1), IsOk());
  EXPECT_THAT(index.Get(2), StatusIs(absl::StatusCode::kNotFound, _));
  EXPECT_THAT(index.Get(3), StatusIs(absl::StatusCode::kNotFound, _));

  EXPECT_THAT(index.GetOrAdd(2), IsOkAndHolds(std::pair(1, true)));
  EXPECT_THAT(index.Get(1), IsOk());
  EXPECT_THAT(index.Get(2), IsOk());
  EXPECT_THAT(index.Get(3), StatusIs(absl::StatusCode::kNotFound, _));
}

TYPED_TEST_P(IndexTest, GetRetrievesPresentKeys) {
  ASSERT_OK_AND_ASSIGN(auto wrapper, IndexHandler<TypeParam>::Create());
  auto& index = wrapper.GetIndex();
  EXPECT_THAT(index.Get(1), StatusIs(absl::StatusCode::kNotFound, _));
  EXPECT_THAT(index.Get(2), StatusIs(absl::StatusCode::kNotFound, _));

  ASSERT_OK_AND_ASSIGN(auto id1, index.GetOrAdd(1));
  EXPECT_THAT(index.Get(1), IsOkAndHolds(id1.first));

  EXPECT_THAT(index.Get(2), StatusIs(absl::StatusCode::kNotFound, _));
  ASSERT_OK_AND_ASSIGN(auto id2, index.GetOrAdd(2));

  EXPECT_THAT(index.Get(2), IsOkAndHolds(id2.first));
  EXPECT_THAT(index.Get(1), IsOkAndHolds(id1.first));
}

TYPED_TEST_P(IndexTest, EmptyIndexHasHashEqualsZero) {
  ASSERT_OK_AND_ASSIGN(auto wrapper, IndexHandler<TypeParam>::Create());
  auto& index = wrapper.GetIndex();
  EXPECT_THAT(index.GetHash(), IsOkAndHolds(Hash{}));
}

TYPED_TEST_P(IndexTest, IndexHashIsEqualToInsertionOrder) {
  Hash hash{};
  ASSERT_OK_AND_ASSIGN(auto wrapper, IndexHandler<TypeParam>::Create());
  auto& index = wrapper.GetIndex();
  EXPECT_THAT(index.GetHash(), IsOkAndHolds(hash));
  ASSERT_OK(index.GetOrAdd(12));
  hash = GetSha256Hash(hash, 12);
  EXPECT_THAT(index.GetHash(), IsOkAndHolds(hash));
  ASSERT_OK(index.GetOrAdd(14));
  hash = GetSha256Hash(hash, 14);
  EXPECT_THAT(index.GetHash(), IsOkAndHolds(hash));
  ASSERT_OK(index.GetOrAdd(16));
  hash = GetSha256Hash(hash, 16);
  EXPECT_THAT(index.GetHash(), IsOkAndHolds(hash));
}

TYPED_TEST_P(IndexTest, CanProduceMemoryFootprint) {
  ASSERT_OK_AND_ASSIGN(auto wrapper, IndexHandler<TypeParam>::Create());
  auto& index = wrapper.GetIndex();
  auto summary = index.GetMemoryFootprint();
  EXPECT_GT(summary.GetTotal(), Memory(0));
}

TYPED_TEST_P(IndexTest, HashesMatchReferenceImplementation) {
  ASSERT_OK_AND_ASSIGN(auto wrapper, IndexHandler<TypeParam>::Create());
  auto& index = wrapper.GetIndex();
  auto& reference_index = wrapper.GetReferenceIndex();

  ASSERT_OK(index.GetOrAdd(1));
  ASSERT_OK(index.GetOrAdd(2));
  ASSERT_OK(index.GetOrAdd(3));

  ASSERT_OK(reference_index.GetOrAdd(1));
  ASSERT_OK(reference_index.GetOrAdd(2));
  ASSERT_OK(reference_index.GetOrAdd(3));

  ASSERT_OK_AND_ASSIGN(auto hash, index.GetHash());
  EXPECT_THAT(reference_index.GetHash(), IsOkAndHolds(hash));
}

REGISTER_TYPED_TEST_SUITE_P(
    IndexTest, TypeProperties, IdentifiersAreAssignedInorder,
    SameKeyLeadsToSameIdentifier, ContainsIdentifiesIndexedElements,
    GetRetrievesPresentKeys, EmptyIndexHasHashEqualsZero,
    IndexHashIsEqualToInsertionOrder, CanProduceMemoryFootprint,
    HashesMatchReferenceImplementation);

using IndexTypes = ::testing::Types<
    InMemoryIndex<int, int>, InMemoryLinearHashIndex<int, int, 16>,
    FileIndex<int, int, InMemoryFile, 128>, MultiLevelDbIndex<int, int>,
    LevelDbKeySpace<int, int>, Cached<InMemoryIndex<int, int>>>;

INSTANTIATE_TYPED_TEST_SUITE_P(All, IndexTest, IndexTypes);

TEST(IndexHashTest, KnownAddresssIndexHashes) {
  InMemoryIndex<Address, int> index;

  ASSERT_OK_AND_ASSIGN(auto hash, index.GetHash());
  EXPECT_THAT(Print(hash),
              StrEq("0x00000000000000000000000000000000000000000000000000000000"
                    "00000000"));

  EXPECT_THAT(index.Get(Address{0x01}),
              StatusIs(absl::StatusCode::kNotFound, _));
  ASSERT_OK(index.GetOrAdd(Address{0x01}));
  ASSERT_OK_AND_ASSIGN(hash, index.GetHash());
  EXPECT_THAT(Print(hash),
              StrEq("0xff9226e320b1deb7fabecff9ac800cd8eb1e3fb7709c003e2effcce3"
                    "7eec68ed"));

  EXPECT_THAT(index.Get(Address{0x02}),
              StatusIs(absl::StatusCode::kNotFound, _));
  ASSERT_OK(index.GetOrAdd(Address{0x02}));
  ASSERT_OK_AND_ASSIGN(hash, index.GetHash());
  EXPECT_THAT(Print(hash),
              StrEq("0xc28553369c52e217564d3f5a783e2643186064498d1b3071568408d4"
                    "9eae6cbe"));
}

TEST(IndexHashTest, KnownKeyIndexHashes) {
  InMemoryIndex<Key, int> index;

  ASSERT_OK_AND_ASSIGN(auto hash, index.GetHash());
  EXPECT_THAT(Print(hash),
              StrEq("0x00000000000000000000000000000000000000000000000000000000"
                    "00000000"));

  EXPECT_THAT(index.Get(Key{0x01}), StatusIs(absl::StatusCode::kNotFound, _));
  ASSERT_OK(index.GetOrAdd(Key{0x01}));
  ASSERT_OK_AND_ASSIGN(hash, index.GetHash());
  EXPECT_THAT(Print(hash),
              StrEq("0xcb592844121d926f1ca3ad4e1d6fb9d8e260ed6e3216361f7732e975"
                    "a0e8bbf6"));

  EXPECT_THAT(index.Get(Key{0x02}), StatusIs(absl::StatusCode::kNotFound, _));
  ASSERT_OK(index.GetOrAdd(Key{0x02}));
  ASSERT_OK_AND_ASSIGN(hash, index.GetHash());
  EXPECT_THAT(Print(hash),
              StrEq("0x975d8dfa71d715cead145c4b80c474d210471dbc7ff614e9dab53887"
                    "d61bc957"));
}

}  // namespace
}  // namespace carmen::backend::index
