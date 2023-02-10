#include "backend/depot/memory/depot.h"

#include "backend/depot/depot.h"
#include "backend/depot/depot_test_suite.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::backend::depot {
namespace {

using TestDepot = InMemoryDepot<unsigned long>;
using DepotTypes = ::testing::Types<
    // Branching size 3, Size of box 1.
    DepotTestConfig<TestDepot, 3, 1>,
    // Branching size 3, Size of box 2.
    DepotTestConfig<TestDepot, 3, 2>,
    // Branching size 16, Size of box 8.
    DepotTestConfig<TestDepot, 16, 8>,
    // Branching size 32, Size of box 16.
    DepotTestConfig<TestDepot, 32, 16>>;

// Instantiates common depot tests for the InMemory depot type.
INSTANTIATE_TYPED_TEST_SUITE_P(LevelDb, DepotTest, DepotTypes);

TEST(InMemoryDepotTest, IsDepot) { EXPECT_TRUE(Depot<InMemoryDepot<int>>); }

}  // namespace
}  // namespace carmen::backend::depot
