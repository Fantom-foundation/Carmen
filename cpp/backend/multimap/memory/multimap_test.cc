#include "backend/multimap/memory/multimap.h"

#include <utility>
#include <vector>

#include "backend/multimap/multimap.h"
#include "common/file_util.h"
#include "common/status_test_util.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::backend::multimap {

using ::testing::IsOkAndHolds;
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
  map.ForEach(key,
              [&](const V& value) {
                res.push_back({key, value});
              })
      .IgnoreError();
  return res;
}

TEST(InMemoryMultiMap, IsMultiMap) {
  EXPECT_TRUE((MultiMap<InMemoryMultiMap<int, int>>));
}

TEST(InMemoryMultiMap, InsertedElementsCanBeFound) {
  TempDir dir;
  Context context;
  ASSERT_OK_AND_ASSIGN(auto map,
                       (InMemoryMultiMap<int, int>::Open(context, dir)));
  EXPECT_THAT(map.Contains(1, 2), IsOkAndHolds(false));
  EXPECT_THAT(map.Contains(1, 3), IsOkAndHolds(false));
  EXPECT_THAT(map.Contains(2, 2), IsOkAndHolds(false));

  EXPECT_THAT(map.Insert(1, 2), IsOkAndHolds(true));
  EXPECT_THAT(map.Contains(1, 2), IsOkAndHolds(true));
  EXPECT_THAT(map.Contains(1, 3), IsOkAndHolds(false));
  EXPECT_THAT(map.Contains(2, 2), IsOkAndHolds(false));

  EXPECT_THAT(map.Insert(1, 3), IsOkAndHolds(true));
  EXPECT_THAT(map.Contains(1, 2), IsOkAndHolds(true));
  EXPECT_THAT(map.Contains(1, 3), IsOkAndHolds(true));
  EXPECT_THAT(map.Contains(2, 2), IsOkAndHolds(false));

  EXPECT_THAT(map.Insert(2, 2), IsOkAndHolds(true));
  EXPECT_THAT(map.Contains(1, 2), IsOkAndHolds(true));
  EXPECT_THAT(map.Contains(1, 3), IsOkAndHolds(true));
  EXPECT_THAT(map.Contains(2, 2), IsOkAndHolds(true));
}

TEST(InMemoryMultiMap, InsertedElementsCanBeEnumerated) {
  using E = std::pair<int, int>;
  TempDir dir;
  Context context;
  ASSERT_OK_AND_ASSIGN(auto map,
                       (InMemoryMultiMap<int, int>::Open(context, dir)));
  EXPECT_THAT(Enumerate(map), UnorderedElementsAre());

  EXPECT_THAT(map.Insert(1, 2), IsOkAndHolds(true));
  EXPECT_THAT(Enumerate(map), UnorderedElementsAre(E{1, 2}));

  EXPECT_THAT(map.Insert(1, 3), IsOkAndHolds(true));
  EXPECT_THAT(Enumerate(map), UnorderedElementsAre(E{1, 2}, E{1, 3}));

  EXPECT_THAT(map.Insert(2, 2), IsOkAndHolds(true));
  EXPECT_THAT(Enumerate(map), UnorderedElementsAre(E{1, 2}, E{1, 3}, E{2, 2}));
}

TEST(InMemoryMultiMap, InsertedElementsCanBeEnumeratedByKey) {
  using E = std::pair<int, int>;
  TempDir dir;
  Context context;
  ASSERT_OK_AND_ASSIGN(auto map,
                       (InMemoryMultiMap<int, int>::Open(context, dir)));
  EXPECT_THAT(Enumerate(1, map), UnorderedElementsAre());
  EXPECT_THAT(Enumerate(2, map), UnorderedElementsAre());

  EXPECT_THAT(map.Insert(1, 2), IsOkAndHolds(true));
  EXPECT_THAT(Enumerate(1, map), UnorderedElementsAre(E{1, 2}));
  EXPECT_THAT(Enumerate(2, map), UnorderedElementsAre());

  EXPECT_THAT(map.Insert(1, 3), IsOkAndHolds(true));
  EXPECT_THAT(Enumerate(1, map), UnorderedElementsAre(E{1, 2}, E{1, 3}));
  EXPECT_THAT(Enumerate(2, map), UnorderedElementsAre());

  EXPECT_THAT(map.Insert(2, 2), IsOkAndHolds(true));
  EXPECT_THAT(Enumerate(1, map), UnorderedElementsAre(E{1, 2}, E{1, 3}));
  EXPECT_THAT(Enumerate(2, map), UnorderedElementsAre(E{2, 2}));
}

TEST(InMemoryMultiMap, SameElementCanNotBeInsertedTwice) {
  using E = std::pair<int, int>;
  TempDir dir;
  Context context;
  ASSERT_OK_AND_ASSIGN(auto map,
                       (InMemoryMultiMap<int, int>::Open(context, dir)));

  EXPECT_THAT(map.Insert(1, 1), IsOkAndHolds(true));
  EXPECT_THAT(Enumerate(1, map), UnorderedElementsAre(E{1, 1}));

  EXPECT_THAT(map.Insert(1, 1), IsOkAndHolds(false));
  EXPECT_THAT(Enumerate(1, map), UnorderedElementsAre(E{1, 1}));
}

TEST(InMemoryMultiMap, ElementsCanBeErasedSelectively) {
  using E = std::pair<int, int>;
  TempDir dir;
  Context context;
  ASSERT_OK_AND_ASSIGN(auto map,
                       (InMemoryMultiMap<int, int>::Open(context, dir)));

  EXPECT_THAT(map.Insert(1, 1), IsOkAndHolds(true));
  EXPECT_THAT(map.Insert(1, 2), IsOkAndHolds(true));
  EXPECT_THAT(map.Insert(2, 3), IsOkAndHolds(true));
  EXPECT_THAT(Enumerate(map), UnorderedElementsAre(E{1, 1}, E{1, 2}, E{2, 3}));

  EXPECT_THAT(map.Erase(1, 2), IsOkAndHolds(true));
  EXPECT_THAT(Enumerate(map), UnorderedElementsAre(E{1, 1}, E{2, 3}));

  EXPECT_THAT(map.Erase(1, 2), IsOkAndHolds(false));
  EXPECT_THAT(Enumerate(map), UnorderedElementsAre(E{1, 1}, E{2, 3}));
}

TEST(InMemoryMultiMap, ElementsCanBeErasedByKey) {
  using E = std::pair<int, int>;
  TempDir dir;
  Context context;
  ASSERT_OK_AND_ASSIGN(auto map,
                       (InMemoryMultiMap<int, int>::Open(context, dir)));

  EXPECT_THAT(map.Insert(1, 1), IsOkAndHolds(true));
  EXPECT_THAT(map.Insert(1, 2), IsOkAndHolds(true));
  EXPECT_THAT(map.Insert(2, 3), IsOkAndHolds(true));
  EXPECT_THAT(Enumerate(map), UnorderedElementsAre(E{1, 1}, E{1, 2}, E{2, 3}));

  EXPECT_OK(map.Erase(1));
  EXPECT_THAT(Enumerate(map), UnorderedElementsAre(E{2, 3}));

  EXPECT_OK(map.Erase(1));
  EXPECT_THAT(Enumerate(map), UnorderedElementsAre(E{2, 3}));
}

TEST(InMemoryMultiMap, NonExistingElementsCanBeErased) {
  TempDir dir;
  Context context;
  ASSERT_OK_AND_ASSIGN(auto map,
                       (InMemoryMultiMap<int, int>::Open(context, dir)));

  EXPECT_OK(map.Erase(1, 2));
  EXPECT_THAT(Enumerate(map), UnorderedElementsAre());
}

TEST(InMemoryMultiMap, CanBeStoredAndReloaded) {
  TempDir dir;
  Context context;

  {
    ASSERT_OK_AND_ASSIGN(auto map,
                         (InMemoryMultiMap<int, int>::Open(context, dir)));
    EXPECT_OK(map.Insert(1, 2));
    EXPECT_OK(map.Insert(1, 3));
    EXPECT_OK(map.Insert(2, 4));
    EXPECT_OK(map.Close());
  }

  {
    using E = std::pair<int, int>;
    ASSERT_OK_AND_ASSIGN(auto map,
                         (InMemoryMultiMap<int, int>::Open(context, dir)));
    EXPECT_THAT(Enumerate(map),
                UnorderedElementsAre(E{1, 2}, E{1, 3}, E{2, 4}));
  }
}

}  // namespace carmen::backend::multimap
