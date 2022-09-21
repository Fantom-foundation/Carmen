#include "backend/index/memory/index.h"

#include <type_traits>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::backend::index {
namespace {

using TestIndex = InMemoryIndex<std::string, int>;

TEST(InMemoryIndexTest, TypeProperties) { 
    EXPECT_TRUE(std::is_default_constructible_v<TestIndex>);
    EXPECT_TRUE(std::is_move_constructible_v<TestIndex>);
}

TEST(InMemoryIndexTest, IdentifiersAreAssignedInorder) { 
    TestIndex index;
    EXPECT_EQ(0, index.GetOrAdd("a"));
    EXPECT_EQ(1, index.GetOrAdd("b"));
    EXPECT_EQ(2, index.GetOrAdd("c"));
}

TEST(InMemoryIndexTest, SameKeyLeadsToSameIdentifier) { 
    TestIndex index;
    EXPECT_EQ(0, index.GetOrAdd("a"));
    EXPECT_EQ(1, index.GetOrAdd("b"));
    EXPECT_EQ(0, index.GetOrAdd("a"));
    EXPECT_EQ(1, index.GetOrAdd("b"));
}

TEST(InMemoryIndexTest, ContainsIdentifiesIndexedElements) { 
    TestIndex index;
    EXPECT_FALSE(index.Contains("a"));
    EXPECT_FALSE(index.Contains("b"));
    EXPECT_FALSE(index.Contains("c"));

    EXPECT_EQ(0, index.GetOrAdd("a"));
    EXPECT_TRUE(index.Contains("a"));
    EXPECT_FALSE(index.Contains("b"));
    EXPECT_FALSE(index.Contains("c"));


    EXPECT_EQ(1, index.GetOrAdd("b"));
    EXPECT_TRUE(index.Contains("a"));
    EXPECT_TRUE(index.Contains("b"));
    EXPECT_FALSE(index.Contains("c"));
}

} // namespace
} // namespace carmen::baclend::index