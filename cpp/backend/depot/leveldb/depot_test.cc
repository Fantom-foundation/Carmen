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

#include "backend/depot/leveldb/depot.h"

#include "backend/depot/depot.h"
#include "backend/depot/depot_test_suite.h"
#include "common/file_util.h"
#include "common/status_test_util.h"
#include "common/type.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::backend::depot {
namespace {

using ::testing::_;
using ::testing::ElementsAreArray;
using ::testing::IsOkAndHolds;
using ::testing::StatusIs;

using TestDepot = LevelDbDepot<unsigned long>;
using DepotTypes = ::testing::Types<
    // Branching size 3, Size of box 1.
    DepotTestConfig<TestDepot, 3, 1>,
    // Branching size 3, Size of box 2.
    DepotTestConfig<TestDepot, 3, 2>,
    // Branching size 16, Size of box 8.
    DepotTestConfig<TestDepot, 16, 8>,
    // Branching size 32, Size of box 16.
    DepotTestConfig<TestDepot, 32, 16>>;

// Instantiates common depot tests for the LevelDb depot type.
INSTANTIATE_TYPED_TEST_SUITE_P(LevelDb, DepotTest, DepotTypes);

TEST(LevelDbDepotTest, IsDepot) { EXPECT_TRUE(Depot<TestDepot>); }

TEST(LevelDbDepotTest, TestIsPersistent) {
  auto dir = TempDir();
  auto elements = std::array{std::byte{1}, std::byte{2}, std::byte{3}};
  Hash hash;

  {
    ASSERT_OK_AND_ASSIGN(auto depot, TestDepot::Open(dir.GetPath()));
    EXPECT_THAT(depot.Get(10), StatusIs(absl::StatusCode::kNotFound, _));
    EXPECT_THAT(depot.GetHash(), IsOkAndHolds(Hash{}));
    ASSERT_OK(depot.Set(10, elements));
    ASSERT_OK_AND_ASSIGN(hash, depot.GetHash());
  }

  {
    ASSERT_OK_AND_ASSIGN(auto depot, TestDepot::Open(dir.GetPath()));
    EXPECT_THAT(depot.Get(10), IsOkAndHolds(ElementsAreArray(elements)));
    EXPECT_THAT(depot.GetHash(), IsOkAndHolds(hash));
  }
}

}  // namespace
}  // namespace carmen::backend::depot
