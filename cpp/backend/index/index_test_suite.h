/*
 * Copyright (c) 2024 Fantom Foundation
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE file and at fantom.foundation/bsl11.
 *
 * Change Date: 2028-4-16
 *
 * On the date above, in accordance with the Business Source License, use of
 * this software will be governed by the GNU Lesser General Public License v3.
 */

#pragma once

#include <filesystem>
#include <type_traits>

#include "absl/status/status.h"
#include "backend/index/index.h"
#include "backend/index/index_handler.h"
#include "common/hash.h"
#include "common/memory_usage.h"
#include "common/status_test_util.h"
#include "common/type.h"
#include "gtest/gtest.h"

namespace carmen::backend::index {

using ::testing::_;
using ::testing::IsOk;
using ::testing::IsOkAndHolds;
using ::testing::Optional;
using ::testing::StatusIs;

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
}  // namespace carmen::backend::index
