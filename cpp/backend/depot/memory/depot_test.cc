#include "backend/depot/memory/depot.h"

#include "backend/depot/depot.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::backend::depot {
namespace {

TEST(InMemoryDepotTest, IsDepot) { EXPECT_TRUE(Depot<InMemoryDepot<int>>); }

}  // namespace
}  // namespace carmen::backend::depot
