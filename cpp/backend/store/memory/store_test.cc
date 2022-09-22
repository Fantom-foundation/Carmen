#include "backend/store/memory/store.h"

#include <type_traits>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::backend::store {
namespace {

using Store = InMemoryStore<int, int>;

TEST(InMemoryStoreTest, TypeTraits) {

}

TEST(InMemoryStoreTest, DataCanBeAddedAndRetrieved) {
    Store store;
    EXPECT_EQ(0, store.Get(10));
    EXPECT_EQ(0, store.Get(12));

    store.Set(10, 12);
    EXPECT_EQ(12, store.Get(10));
    EXPECT_EQ(0, store.Get(12));

    store.Set(12, 14);
    EXPECT_EQ(12, store.Get(10));
    EXPECT_EQ(14, store.Get(12));
}

TEST(InMemoryStoreTest, EntriesCanBeUpdated) {
    Store store;
    EXPECT_EQ(0, store.Get(10));
    store.Set(10, 12);
    EXPECT_EQ(12, store.Get(10));
    store.Set(10, 14);
    EXPECT_EQ(14, store.Get(10));
}

TEST(InMemoryStoreTest, DefaultValueIsEnforced) {
    Store store(/*default_value=*/8);
    EXPECT_EQ(8, store.Get(10));
    store.Set(10, 12);
    EXPECT_EQ(12, store.Get(10));
}

}
}  // namespace carmen::backend::store