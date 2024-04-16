/*
 * Copyright (c) 2024 Fantom Foundation
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at fantom.foundation/bsl11.
 *
 * Change Date: 2028-4-16
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by the GNU Lesser General Public Licence v3.
 */

#include "backend/common/btree/btree_map.h"

#include "backend/common/btree/test_util.h"
#include "backend/common/file.h"
#include "backend/common/page_pool.h"
#include "common/file_util.h"
#include "common/status_test_util.h"

namespace carmen::backend {
namespace {

using ::testing::FieldsAre;
using ::testing::IsOkAndHolds;
using ::testing::Optional;
using ::testing::Pointee;

using TestPagePool = PagePool<InMemoryFile<kFileSystemPageSize>>;

template <Trivial Key, Trivial Value, std::size_t max_keys = 0,
          std::size_t max_elements = 0>
using TestBTreeMap = BTreeMap<Key, Value, TestPagePool, std::less<Value>,
                              max_keys, max_elements>;

TEST(BTreeMap, EmptySetContainsNothing) {
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto map, (TestBTreeMap<int, int>::Open(dir)));

  EXPECT_THAT(map.Contains(0), false);
  EXPECT_THAT(map.Contains(1), false);
  EXPECT_THAT(map.Contains(7), false);
  EXPECT_THAT(map.Contains(92), false);

  ASSERT_OK_AND_ASSIGN(auto begin, map.Begin());
  ASSERT_OK_AND_ASSIGN(auto end, map.End());
  EXPECT_EQ(begin, end);
}

TEST(BTreeMap, InsertedElementsCanBeFound) {
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto map, (TestBTreeMap<int, int>::Open(dir)));
  EXPECT_THAT(map.Contains(12), false);
  EXPECT_THAT(map.Contains(14), false);
  EXPECT_THAT(map.Insert(12, 14), true);
  EXPECT_THAT(map.Contains(12), true);
  EXPECT_THAT(map.Contains(14), false);
}

TEST(BTreeMap, ValuesAssociatedToKeysCanBetFound) {
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto map, (TestBTreeMap<int, int>::Open(dir)));
  EXPECT_THAT(map.Insert(1, 2), true);
  EXPECT_THAT(map.Insert(2, 3), true);

  EXPECT_THAT(map.Find(1), IsOkAndHolds(Pointee(FieldsAre(1, 2))));
  EXPECT_THAT(map.Find(2), IsOkAndHolds(Pointee(FieldsAre(2, 3))));
}

template <typename Tree>
void RunInsertionAndLookupTest(const std::vector<int>& data) {
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto set, Tree::Open(dir));
  for (int i : data) {
    EXPECT_THAT(set.Insert(i, 2 * i), true);
    if (auto check = set.Check(); !check.ok()) {
      std::cout << "After inserting " << i << " ...\n";
      set.Print();
      ASSERT_OK(check);
    }
  }
  for (int i : data) {
    EXPECT_THAT(set.Find(i), IsOkAndHolds(Pointee(FieldsAre(i, 2 * i))))
        << "i=" << i;
  }
}

TEST(BTreeMap, OrderedInsertsRetainInvariants) {
  RunInsertionAndLookupTest<TestBTreeMap<int, int>>(GetSequence(10000));
}

TEST(BTreeMap, OrderedInsertsRetainInvariantsInNarrowTreeWithEvenBranching) {
  // This is the same as above, but with a tree with a reduced branch width to
  // stress-test edge cases and splitting.
  RunInsertionAndLookupTest<TestBTreeMap<int, int, 6, 6>>(GetSequence(10000));
}

TEST(BTreeMap, OrderedInsertsRetainInvariantsInNarrowTreeWithOddBranching) {
  // Same as above, but with an odd number of keys / elements.
  RunInsertionAndLookupTest<TestBTreeMap<int, int, 7, 7>>(GetSequence(10000));
}

TEST(BTreeMap, RandomInsertsRetainInvariants) {
  RunInsertionAndLookupTest<TestBTreeMap<int, int>>(
      Shuffle(GetSequence(10000)));
}

TEST(BTreeMap, RandomInsertsRetainInvariantsInNarrowTreeWithEvenBranching) {
  RunInsertionAndLookupTest<TestBTreeMap<int, int, 6, 6>>(
      Shuffle(GetSequence(10000)));
}

TEST(BTreeMap, RandomInsertsRetainInvariantsInNarrowTreeWithOddBranching) {
  RunInsertionAndLookupTest<TestBTreeMap<int, int, 7, 7>>(
      Shuffle(GetSequence(10000)));
}

template <typename Map>
void RunClosingAndReopeningTest() {
  const int N = 10000;
  const int S = 3;
  const int K = 5;
  TempFile file;
  std::size_t size;

  // Create a map containing some elements.
  {
    ASSERT_OK_AND_ASSIGN(auto map, Map::Open(file));
    EXPECT_OK(map.Check());
    for (int i = 0; i < N; i += S) {
      ASSERT_OK(map.Insert(i, 2 * i));
    }
    EXPECT_OK(map.Check());
    size = map.Size();
    EXPECT_OK(map.Close());
  }

  // Reopen the map, check content, and add additional elements.
  {
    ASSERT_OK_AND_ASSIGN(auto map, Map::Open(file));
    EXPECT_OK(map.Check());
    EXPECT_EQ(map.Size(), size);
    for (int i = 0; i < N; i++) {
      bool should_exist = (i % S == 0);
      EXPECT_THAT(map.Contains(i), should_exist) << "i=" << i;
      if (should_exist) {
        EXPECT_THAT(map.Find(i), IsOkAndHolds(Pointee(FieldsAre(i, 2 * i))))
            << "i=" << i;
        ;
      } else {
        EXPECT_THAT(map.Find(i), map.End());
      }
    }
    for (int i = 0; i < N; i += K) {
      EXPECT_THAT(map.Insert(i, 2 * i), !(i % S == 0));
    }
    EXPECT_OK(map.Check());
    size = map.Size();
    EXPECT_OK(map.Close());
  }

  // Reopen a second time to see whether insert on the reopened map was
  // successful.
  {
    ASSERT_OK_AND_ASSIGN(auto map, Map::Open(file));
    EXPECT_OK(map.Check());
    EXPECT_EQ(map.Size(), size);
    for (int i = 0; i < N; i++) {
      bool should_exist = (i % S == 0 || i % K == 0);
      EXPECT_THAT(map.Contains(i), should_exist) << "i=" << i;
      if (should_exist) {
        EXPECT_THAT(map.Find(i), IsOkAndHolds(Pointee(FieldsAre(i, 2 * i))))
            << "i=" << i;
        ;
      } else {
        EXPECT_THAT(map.Find(i), map.End());
      }
    }
    EXPECT_OK(map.Close());
  }
}

TEST(BTreeMap, ClosingAndReopeningProducesSameMap) {
  using Pool = PagePool<SingleFile<kFileSystemPageSize>>;
  // Run the test with the maximum number of keys per node.
  RunClosingAndReopeningTest<BTreeMap<int, int, Pool>>();
  // Run the tests with small even/odd numbers of keys to test deeper trees.
  RunClosingAndReopeningTest<BTreeMap<int, int, Pool, std::less<int>, 2, 2>>();
  RunClosingAndReopeningTest<BTreeMap<int, int, Pool, std::less<int>, 3, 3>>();
  RunClosingAndReopeningTest<
      BTreeMap<int, int, Pool, std::less<int>, 11, 10>>();
  RunClosingAndReopeningTest<
      BTreeMap<int, int, Pool, std::less<int>, 10, 11>>();
}

}  // namespace
}  // namespace carmen::backend
