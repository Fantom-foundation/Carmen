#include "backend/multimap/memory/multimap.h"

#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::backend::multimap {

using ::testing::UnorderedElementsAre;

template <typename K, typename V>
std::vector<std::pair<K, V>> Enumerate(const InMemoryMultiMap<K, V>& map) {
  std::vector<std::pair<K, V>> res;
  map.ForEach([&](const K& key, const V& value) {
    res.push_back({key, value});
  });
  return res;
}

template <typename K, typename V>
std::vector<std::pair<K, V>> Enumerate(const K& key,
                                       const InMemoryMultiMap<K, V>& map) {
  std::vector<std::pair<K, V>> res;
  map.ForEach(key, [&](const V& value) { res.push_back({key, value}); });
  return res;
}

TEST(InMemoryMultiMap, InsertedElementsCanBeFound) {
  InMemoryMultiMap<int, int> map;
  EXPECT_FALSE(map.Contains(1, 2));
  EXPECT_FALSE(map.Contains(1, 3));
  EXPECT_FALSE(map.Contains(2, 2));

  EXPECT_TRUE(map.Insert(1, 2));
  EXPECT_TRUE(map.Contains(1, 2));
  EXPECT_FALSE(map.Contains(1, 3));
  EXPECT_FALSE(map.Contains(2, 2));

  EXPECT_TRUE(map.Insert(1, 3));
  EXPECT_TRUE(map.Contains(1, 2));
  EXPECT_TRUE(map.Contains(1, 3));
  EXPECT_FALSE(map.Contains(2, 2));

  EXPECT_TRUE(map.Insert(2, 2));
  EXPECT_TRUE(map.Contains(1, 2));
  EXPECT_TRUE(map.Contains(1, 3));
  EXPECT_TRUE(map.Contains(2, 2));
}

TEST(InMemoryMultiMap, InsertedElementsCanBeEnumerated) {
  using E = std::pair<int, int>;
  InMemoryMultiMap<int, int> map;
  EXPECT_THAT(Enumerate(map), UnorderedElementsAre());

  EXPECT_TRUE(map.Insert(1, 2));
  EXPECT_THAT(Enumerate(map), UnorderedElementsAre(E{1, 2}));

  EXPECT_TRUE(map.Insert(1, 3));
  EXPECT_THAT(Enumerate(map), UnorderedElementsAre(E{1, 2}, E{1, 3}));

  EXPECT_TRUE(map.Insert(2, 2));
  EXPECT_THAT(Enumerate(map), UnorderedElementsAre(E{1, 2}, E{1, 3}, E{2, 2}));
}

TEST(InMemoryMultiMap, InsertedElementsCanBeEnumeratedByKey) {
  using E = std::pair<int, int>;
  InMemoryMultiMap<int, int> map;
  EXPECT_THAT(Enumerate(1, map), UnorderedElementsAre());
  EXPECT_THAT(Enumerate(2, map), UnorderedElementsAre());

  EXPECT_TRUE(map.Insert(1, 2));
  EXPECT_THAT(Enumerate(1, map), UnorderedElementsAre(E{1, 2}));
  EXPECT_THAT(Enumerate(2, map), UnorderedElementsAre());

  EXPECT_TRUE(map.Insert(1, 3));
  EXPECT_THAT(Enumerate(1, map), UnorderedElementsAre(E{1, 2}, E{1, 3}));
  EXPECT_THAT(Enumerate(2, map), UnorderedElementsAre());

  EXPECT_TRUE(map.Insert(2, 2));
  EXPECT_THAT(Enumerate(1, map), UnorderedElementsAre(E{1, 2}, E{1, 3}));
  EXPECT_THAT(Enumerate(2, map), UnorderedElementsAre(E{2, 2}));
}

TEST(InMemoryMultiMap, SameElementCanNotBeInsertedTwice) {
  using E = std::pair<int, int>;
  InMemoryMultiMap<int, int> map;

  EXPECT_TRUE(map.Insert(1, 1));
  EXPECT_THAT(Enumerate(1, map), UnorderedElementsAre(E{1, 1}));

  EXPECT_FALSE(map.Insert(1, 1));
  EXPECT_THAT(Enumerate(1, map), UnorderedElementsAre(E{1, 1}));
}

TEST(InMemoryMultiMap, ElementsCanBeErasedSelectively) {
  using E = std::pair<int, int>;
  InMemoryMultiMap<int, int> map;

  EXPECT_TRUE(map.Insert(1, 1));
  EXPECT_TRUE(map.Insert(1, 2));
  EXPECT_TRUE(map.Insert(2, 3));
  EXPECT_THAT(Enumerate(map), UnorderedElementsAre(E{1, 1}, E{1, 2}, E{2, 3}));

  map.Erase(1, 2);
  EXPECT_THAT(Enumerate(map), UnorderedElementsAre(E{1, 1}, E{2, 3}));
}

TEST(InMemoryMultiMap, ElementsCanBeErasedByKey) {
  using E = std::pair<int, int>;
  InMemoryMultiMap<int, int> map;

  EXPECT_TRUE(map.Insert(1, 1));
  EXPECT_TRUE(map.Insert(1, 2));
  EXPECT_TRUE(map.Insert(2, 3));
  EXPECT_THAT(Enumerate(map), UnorderedElementsAre(E{1, 1}, E{1, 2}, E{2, 3}));

  map.Erase(1);
  EXPECT_THAT(Enumerate(map), UnorderedElementsAre(E{2, 3}));
}

TEST(InMemoryMultiMap, NonExistingElementsCanBeErased) {
  InMemoryMultiMap<int, int> map;

  map.Erase(1, 2);
  EXPECT_THAT(Enumerate(map), UnorderedElementsAre());
}

}  // namespace carmen::backend::multimap
