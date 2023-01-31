#include "backend/common/btree/btree_set.h"

#include <algorithm>
#include <initializer_list>
#include <ostream>
#include <vector>

#include "backend/common/file.h"
#include "backend/common/page_pool.h"
#include "common/file_util.h"
#include "common/status_test_util.h"

namespace carmen::backend {
namespace {

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

std::vector<int> GetSequence(int size) {
  std::vector<int> data;
  for (int i = 0; i < size; i++) {
    data.push_back(i);
  }
  return data;
}

std::vector<int> Shuffle(std::vector<int> data) {
  std::random_device rd;
  std::mt19937 g(rd());
  std::shuffle(data.begin(), data.end(), g);
  return data;
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
      ASSERT_OK(set.Insert(i));
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
