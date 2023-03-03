#include "backend/store/memory/store.h"

#include <type_traits>

#include "backend/store/store_test_suite.h"
#include "common/type.h"
#include "gtest/gtest.h"

namespace carmen::backend::store {
namespace {

using Store = InMemoryStore<int, int>;

using StoreTypes = ::testing::Types<
    // Page size 32, branching size 32.
    StoreTestConfig<InMemoryStore, 32, 32>,
    // Page size 64, branching size 3.
    StoreTestConfig<InMemoryStore, 64, 3>,
    // Page size 64, branching size 8.
    StoreTestConfig<InMemoryStore, 64, 8>,
    // Page size 128, branching size 4.
    StoreTestConfig<InMemoryStore, 128, 4>>;

// Instantiates common store tests for the InMemory store type.
INSTANTIATE_TYPED_TEST_SUITE_P(InMemory, StoreTest, StoreTypes);

TEST(InMemoryStoreTest, TypeTraits) {
  EXPECT_TRUE(std::is_default_constructible_v<Store>);
  EXPECT_FALSE(std::is_copy_constructible_v<Store>);
  EXPECT_TRUE(std::is_move_constructible_v<Store>);
  EXPECT_FALSE(std::is_copy_assignable_v<Store>);
  EXPECT_FALSE(std::is_move_assignable_v<Store>);
}

}  // namespace
}  // namespace carmen::backend::store
