/*
 * Copyright (c) 2024 Fantom Foundation
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE file and at fantom.foundation/bsl11.
 *
 * Change Date: 2028-4-16
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by the GNU Lesser General Public Licence v3.
 */

#include "backend/common/btree/btree_set.h"

#include <vector>

#include "backend/common/btree/test_util.h"
#include "backend/common/file.h"
#include "backend/common/page_pool.h"
#include "common/file_util.h"
#include "common/status_test_util.h"

namespace carmen::backend {
namespace {

using ::testing::ElementsAre;
using ::testing::ElementsAreArray;
using ::testing::IsOkAndHolds;

using TestPagePool = PagePool<InMemoryFile<kFileSystemPageSize>>;

template <Trivial Value, std::size_t max_keys = 0, std::size_t max_elements = 0>
using TestBTreeSet =
    BTreeSet<Value, TestPagePool, std::less<Value>, max_keys, max_elements>;

TEST(BTreeSet, EmptySetContainsNothing) {
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto set, TestBTreeSet<int>::Open(dir));

  EXPECT_THAT(set.Contains(0), false);
  EXPECT_THAT(set.Contains(1), false);
  EXPECT_THAT(set.Contains(7), false);
  EXPECT_THAT(set.Contains(92), false);
}

TEST(BTreeSet, InsertedElementsCanBeFound) {
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto set, TestBTreeSet<int>::Open(dir));
  EXPECT_THAT(set.Contains(12), false);
  EXPECT_THAT(set.Insert(12), true);
  EXPECT_THAT(set.Contains(12), true);
}

TEST(BTreeSet, InsertingZeroWorks) {
  TempDir dir;
  // This was once broken since zero is the default value.
  ASSERT_OK_AND_ASSIGN(auto set, TestBTreeSet<int>::Open(dir));
  EXPECT_THAT(set.Contains(0), false);
  EXPECT_THAT(set.Insert(0), true);
  EXPECT_THAT(set.Contains(0), true);
}

TEST(BTreeSet, ElementsCanBeIteratedInForwardOrder) {
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto set, (TestBTreeSet<int, 7, 7>::Open(dir)));
  auto data = GetSequence(1000);
  for (int cur : data) {
    ASSERT_THAT(set.Insert(cur), true);
  }
  ASSERT_OK_AND_ASSIGN(auto begin, set.Begin());
  ASSERT_OK_AND_ASSIGN(auto end, set.End());
  std::vector<int> res;
  res.reserve(set.Size());
  for (auto it = begin; it != end;) {
    res.push_back(*it);
    ASSERT_OK(it.Next());
  }
  EXPECT_THAT(res, ElementsAreArray(data));
}

TEST(BTreeSet, ElementsCanBeIteratedInBackwardOrder) {
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto set, (TestBTreeSet<int, 7, 7>::Open(dir)));
  auto data = GetSequence(1000);
  for (int cur : data) {
    ASSERT_THAT(set.Insert(cur), true);
  }
  ASSERT_OK_AND_ASSIGN(auto begin, set.Begin());
  ASSERT_OK_AND_ASSIGN(auto end, set.End());
  std::vector<int> res;
  res.reserve(set.Size());
  for (auto it = end; it != begin;) {
    ASSERT_OK(it.Previous());
    res.push_back(*it);
  }
  std::reverse(res.begin(), res.end());
  EXPECT_THAT(res, ElementsAreArray(data));
}

TEST(BTreeSet, IteratorReturnedByFindCanBeUsedToNavigate) {
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto set, (TestBTreeSet<int, 7, 7>::Open(dir)));
  for (int cur : GetSequence(100)) {
    ASSERT_THAT(set.Insert(cur), true);
  }
  ASSERT_OK_AND_ASSIGN(auto begin, set.Begin());
  ASSERT_OK_AND_ASSIGN(auto end, set.End());

  EXPECT_THAT(set.Find(0), begin);

  for (int i = 0; i < 100; i++) {
    ASSERT_OK_AND_ASSIGN(auto pos, set.Find(i));
    EXPECT_EQ(*pos, i);
    if (i != 0) {
      auto priv = pos;
      EXPECT_OK(priv.Previous());
      EXPECT_EQ(*priv, i - 1);
    }
    auto next = pos;
    EXPECT_OK(next.Next());
    if (i != 99) {
      EXPECT_EQ(*next, i + 1);
    } else {
      EXPECT_EQ(next, end);
    }
  }
}

template <typename Tree>
void RunInsertionTest(const std::vector<int>& data) {
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto set, Tree::Open(dir));
  for (int i : data) {
    EXPECT_THAT(set.Insert(i), true);
    if (auto check = set.Check(); !check.ok()) {
      std::cout << "After inserting " << i << " ...\n";
      set.Print();
      ASSERT_OK(check);
    }
  }
}

TEST(BTreeSet, OrderedInsertsRetainInvariants) {
  RunInsertionTest<TestBTreeSet<int>>(GetSequence(10000));
}

TEST(BTreeSet, OrderedInsertsRetainInvariantsInNarrowTreeWithEvenBranching) {
  // This is the same as above, but with a tree with a reduced branch width to
  // stress-test edge cases and splitting.
  RunInsertionTest<TestBTreeSet<int, 6, 6>>(GetSequence(10000));
}

TEST(BTreeSet, OrderedInsertsRetainInvariantsInNarrowTreeWithOddBranching) {
  // Same as above, but with an odd number of keys / elements.
  RunInsertionTest<TestBTreeSet<int, 7, 7>>(GetSequence(10000));
}

TEST(BTreeSet, RandomInsertsRetainInvariants) {
  RunInsertionTest<TestBTreeSet<int>>(Shuffle(GetSequence(10000)));
}

TEST(BTreeSet, RandomInsertsRetainInvariantsInNarrowTreeWithEvenBranching) {
  RunInsertionTest<TestBTreeSet<int, 6, 6>>(Shuffle(GetSequence(10000)));
}

TEST(BTreeSet, RandomInsertsRetainInvariantsInNarrowTreeWithOddBranching) {
  RunInsertionTest<TestBTreeSet<int, 7, 7>>(Shuffle(GetSequence(10000)));
}

template <typename Set>
void RunClosingAndReopeningTest() {
  const int N = 10000;
  const int S = 3;
  const int K = 5;
  TempFile file;
  std::size_t size;

  // Create a set containing some elements.
  {
    ASSERT_OK_AND_ASSIGN(auto set, Set::Open(file));
    EXPECT_OK(set.Check());
    for (int i = 0; i < N; i += S) {
      ASSERT_OK(set.Insert(i));
    }
    EXPECT_OK(set.Check());
    size = set.Size();
    EXPECT_OK(set.Close());
  }

  // Reopen the set, check content, and add additional elements.
  {
    ASSERT_OK_AND_ASSIGN(auto set, Set::Open(file));
    EXPECT_OK(set.Check());
    EXPECT_EQ(set.Size(), size);
    for (int i = 0; i < N; i++) {
      EXPECT_THAT(set.Contains(i), i % S == 0) << "i=" << i;
    }
    for (int i = 0; i < N; i += K) {
      EXPECT_THAT(set.Insert(i), !(i % S == 0));
    }
    EXPECT_OK(set.Check());
    size = set.Size();
    EXPECT_OK(set.Close());
  }

  // Reopen a second time to see whether insert on the reopened set was
  // successful.
  {
    ASSERT_OK_AND_ASSIGN(auto set, Set::Open(file));
    EXPECT_OK(set.Check());
    EXPECT_EQ(set.Size(), size);
    for (int i = 0; i < N; i++) {
      EXPECT_THAT(set.Contains(i), i % S == 0 || i % K == 0) << "i=" << i;
    }
    EXPECT_OK(set.Close());
  }
}

TEST(BTreeSet, ClosingAndReopeningProducesSameSet) {
  using Pool = PagePool<SingleFile<kFileSystemPageSize>>;
  // Run the test with the maximum number of keys per node.
  RunClosingAndReopeningTest<BTreeSet<int, Pool>>();
  // Run the tests with small even/odd numbers of keys to test deeper trees.
  RunClosingAndReopeningTest<BTreeSet<int, Pool, std::less<int>, 2, 2>>();
  RunClosingAndReopeningTest<BTreeSet<int, Pool, std::less<int>, 3, 3>>();
  RunClosingAndReopeningTest<BTreeSet<int, Pool, std::less<int>, 11, 10>>();
  RunClosingAndReopeningTest<BTreeSet<int, Pool, std::less<int>, 10, 11>>();
}

}  // namespace
}  // namespace carmen::backend
