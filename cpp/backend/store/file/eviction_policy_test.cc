#include "backend/store/file/eviction_policy.h"

#include <filesystem>
#include <sstream>

#include "gtest/gtest.h"

namespace carmen::backend::store {
namespace {

TEST(RandomEvictionPolicyTest, IsEvictionPolicy) {
  EXPECT_TRUE(EvictionPolicy<RandomEvictionPolicy>);
}

TEST(RandomEvictionPolicyTest, ReturnsNullOptIfNothingIsUsed) {
  RandomEvictionPolicy policy;
  EXPECT_EQ(policy.GetPageToEvict(), std::nullopt);
}

TEST(RandomEvictionPolicyTest, EvictsCleanPagesFirstFollowedByDirty) {
  RandomEvictionPolicy policy;
  policy.Read(10);
  policy.Written(11);
  EXPECT_EQ(policy.GetPageToEvict(), 10);
  policy.Removed(10);
  EXPECT_EQ(policy.GetPageToEvict(), 11);
  policy.Removed(11);
  EXPECT_EQ(policy.GetPageToEvict(), std::nullopt);
}

TEST(LeastRecentlyUsedEvictionPolicyTest, IsEvictionPolicy) {
  EXPECT_TRUE(EvictionPolicy<LeastRecentlyUsedEvictionPolicy>);
}

TEST(LeastRecentlyUsedEvictionPolicyTest, ReturnsNullOptIfNothingIsUsed) {
  LeastRecentlyUsedEvictionPolicy policy;
  EXPECT_EQ(policy.GetPageToEvict(), std::nullopt);
}

TEST(LeastRecentlyUsedEvictionPolicyTest, InOrderReadsAreEvictedInOrder) {
  LeastRecentlyUsedEvictionPolicy policy;

  for (std::size_t i = 0; i < 10; i++) {
    // Adding i pages in order 0 ... i-1.
    for (std::size_t j = 0; j < i; j++) {
      policy.Read(j);
    }

    // Pages should be evicted in same order as added.
    for (std::size_t j = 0; j < i; j++) {
      EXPECT_EQ(j, policy.GetPageToEvict());
      policy.Removed(j);
    }
  }
}

TEST(LeastRecentlyUsedEvictionPolicyTest, LeastRecentlyUsedAreEvicted) {
  LeastRecentlyUsedEvictionPolicy policy;

  EXPECT_EQ(policy.GetPageToEvict(), std::nullopt);
  policy.Read(1);  // now: 1
  EXPECT_EQ(policy.GetPageToEvict(), 1);
  policy.Read(2);  // now: 2, 1
  EXPECT_EQ(policy.GetPageToEvict(), 1);
  policy.Read(3);  // now: 3, 2, 1
  EXPECT_EQ(policy.GetPageToEvict(), 1);

  // Access last.
  policy.Read(1);  // now: 1, 3, 2
  EXPECT_EQ(policy.GetPageToEvict(), 2);

  // Access middle.
  policy.Read(3);  // now 3, 1, 2
  EXPECT_EQ(policy.GetPageToEvict(), 2);

  // Access middle.
  policy.Read(3);  // now 3, 1, 2
  EXPECT_EQ(policy.GetPageToEvict(), 2);

  // Check order.
  policy.Read(2);  // now 2, 3, 1
  EXPECT_EQ(policy.GetPageToEvict(), 1);
  policy.Read(1);  // now 1, 2, 3
  EXPECT_EQ(policy.GetPageToEvict(), 3);
}

TEST(LeastRecentlyUsedEvictionPolicyTest, LastElementCanBeRemoved) {
  LeastRecentlyUsedEvictionPolicy policy;

  EXPECT_EQ(policy.GetPageToEvict(), std::nullopt);
  policy.Read(1);  // now: 1
  EXPECT_EQ(policy.GetPageToEvict(), 1);
  policy.Read(2);  // now: 2, 1
  EXPECT_EQ(policy.GetPageToEvict(), 1);
  policy.Read(3);  // now: 3, 2, 1
  EXPECT_EQ(policy.GetPageToEvict(), 1);

  // Remove last elements.
  policy.Removed(1);  // now: 3, 2
  EXPECT_EQ(policy.GetPageToEvict(), 2);

  policy.Removed(2);  // now 3
  EXPECT_EQ(policy.GetPageToEvict(), 3);

  policy.Removed(3);  // now empty
  EXPECT_EQ(policy.GetPageToEvict(), std::nullopt);
}

TEST(LeastRecentlyUsedEvictionPolicyTest, FirstElementCanBeRemoved) {
  LeastRecentlyUsedEvictionPolicy policy;

  EXPECT_EQ(policy.GetPageToEvict(), std::nullopt);
  policy.Read(1);  // now: 1
  EXPECT_EQ(policy.GetPageToEvict(), 1);
  policy.Read(2);  // now: 2, 1
  EXPECT_EQ(policy.GetPageToEvict(), 1);
  policy.Read(3);  // now: 3, 2, 1
  EXPECT_EQ(policy.GetPageToEvict(), 1);

  // Remove last elements.
  policy.Removed(3);  // now: 2, 1
  EXPECT_EQ(policy.GetPageToEvict(), 1);

  policy.Removed(2);  // now 1
  EXPECT_EQ(policy.GetPageToEvict(), 1);

  policy.Removed(1);  // now empty
  EXPECT_EQ(policy.GetPageToEvict(), std::nullopt);
}

TEST(LeastRecentlyUsedEvictionPolicyTest, MiddleElementCanBeRemoved) {
  LeastRecentlyUsedEvictionPolicy policy;

  EXPECT_EQ(policy.GetPageToEvict(), std::nullopt);
  policy.Read(1);  // now: 1
  EXPECT_EQ(policy.GetPageToEvict(), 1);
  policy.Read(2);  // now: 2, 1
  EXPECT_EQ(policy.GetPageToEvict(), 1);
  policy.Read(3);  // now: 3, 2, 1
  EXPECT_EQ(policy.GetPageToEvict(), 1);

  // Remove middle elements.
  policy.Removed(2);  // now: 3, 1
  EXPECT_EQ(policy.GetPageToEvict(), 1);

  policy.Removed(1);  // now 3
  EXPECT_EQ(policy.GetPageToEvict(), 3);

  policy.Removed(3);  // now empty
  EXPECT_EQ(policy.GetPageToEvict(), std::nullopt);
}

}  // namespace
}  // namespace carmen::backend::store
