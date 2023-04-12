#include "state/s4/nodes.h"

#include <algorithm>
#include <numeric>
#include <random>

#include "common/status_test_util.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::s4 {
namespace {

using ::testing::Eq;
using ::testing::PrintToString;
using ::testing::StrEq;

TEST(NodeId, EmptyIsIdentifiedAsEmpty) {
  auto id = NodeId::Empty();
  EXPECT_EQ(id.GetIndex(), 0);
  EXPECT_TRUE(id.IsEmpty());
  EXPECT_FALSE(id.IsLeaf());
  EXPECT_FALSE(id.IsBranch());
  EXPECT_FALSE(id.IsExtension());
}

TEST(NodeId, LeafIdsAreIdentifiedAsLeafIds) {
  for (int i = 0; i < 100; i++) {
    auto id = NodeId::Leaf(i);
    EXPECT_EQ(id.GetIndex(), i);
    EXPECT_FALSE(id.IsEmpty());
    EXPECT_TRUE(id.IsLeaf());
    EXPECT_FALSE(id.IsBranch());
    EXPECT_FALSE(id.IsExtension());
  }
}

TEST(NodeId, BranchIdsAreIdentifiedAsBranchIds) {
  for (int i = 0; i < 100; i++) {
    auto id = NodeId::Branch(i);
    EXPECT_EQ(id.GetIndex(), i);
    EXPECT_FALSE(id.IsEmpty());
    EXPECT_FALSE(id.IsLeaf());
    EXPECT_TRUE(id.IsBranch());
    EXPECT_FALSE(id.IsExtension());
  }
}

TEST(NodeId, ExtensionIdsAreIdentifiedAsExtensionIds) {
  for (int i = 0; i < 100; i++) {
    auto id = NodeId::Extension(i);
    EXPECT_EQ(id.GetIndex(), i);
    EXPECT_FALSE(id.IsEmpty());
    EXPECT_FALSE(id.IsLeaf());
    EXPECT_FALSE(id.IsBranch());
    EXPECT_TRUE(id.IsExtension());
  }
}

TEST(PathSegment, CanBeConstructedFromNibbles) {
  EXPECT_THAT(PrintToString(PathSegment<16>()), StrEq("0000 : 0"));

  EXPECT_THAT(PrintToString(PathSegment<16>(0x0)), StrEq("0000 : 4"));
  EXPECT_THAT(PrintToString(PathSegment<16>(0x1)), StrEq("0001 : 4"));
  EXPECT_THAT(PrintToString(PathSegment<16>(0x3)), StrEq("0003 : 4"));
  EXPECT_THAT(PrintToString(PathSegment<16>(0x7)), StrEq("0007 : 4"));
  EXPECT_THAT(PrintToString(PathSegment<16>(0xF)), StrEq("000f : 4"));

  EXPECT_THAT(PrintToString(PathSegment<16>({0x1, 0x2})), StrEq("0012 : 8"));
  EXPECT_THAT(PrintToString(PathSegment<16>({0x1, 0x2, 0x3})),
              StrEq("0123 : 12"));
}

TEST(PathSegment, CanBeConstructedFromKeys) {
  auto toSegment = [](int x) -> std::string {
    return PrintToString(PathSegment<16>(x, std::bitset<16>(-1)));
  };

  EXPECT_THAT(toSegment(0), StrEq("0000 : 0"));
  EXPECT_THAT(toSegment(4), StrEq("000f : 4"));
  EXPECT_THAT(toSegment(8), StrEq("00ff : 8"));
  EXPECT_THAT(toSegment(12), StrEq("0fff : 12"));
  EXPECT_THAT(toSegment(16), StrEq("ffff : 16"));
}

TEST(PathSegment, PrepentAddsNibblesToTheFront) {
  PathSegment<16> segment;
  EXPECT_THAT(PrintToString(segment), StrEq("0000 : 0"));
  segment.Prepend(7);
  EXPECT_THAT(PrintToString(segment), StrEq("0007 : 4"));
  segment.Prepend(5);
  EXPECT_THAT(PrintToString(segment), StrEq("0057 : 8"));
  segment.Prepend(14);
  EXPECT_THAT(PrintToString(segment), StrEq("0e57 : 12"));
}

TEST(PathSegment, PrepentingSegmentsConcatenatesSegments) {
  PathSegment<32> seg123({1, 2, 3});
  PathSegment<32> seg45({4, 5});

  auto concat = [](auto a, auto b) -> auto{
    b.Prepend(a);
    return b;
  };

  EXPECT_EQ(concat(seg123, seg45), PathSegment<32>({1, 2, 3, 4, 5}));
  EXPECT_EQ(concat(seg45, seg123), PathSegment<32>({4, 5, 1, 2, 3}));
}

TEST(PathSegment, GetNibbleReturnsProperValue) {
  PathIterator<32> iter(std::bitset<32>(1234567890));
  PathSegment<32> full = iter.GetRemaining();
  for (int i = 0; i < 8; i++) {
    EXPECT_EQ(iter.Next().ToUint(), int(full.GetNibble(i)))
        << "i=" << i << " / " << full.GetNibble(i);
  }
}

TEST(PathSegment, IsPrefixOf) {
  PathSegment<16> seg({});
  PathSegment<16> seg1({1});
  PathSegment<16> seg12({1, 2});
  PathSegment<16> seg123({1, 2, 3});
  PathSegment<16> seg2({2});

  EXPECT_TRUE(seg.IsPrefixOf(seg));
  EXPECT_TRUE(seg.IsPrefixOf(seg1));
  EXPECT_TRUE(seg.IsPrefixOf(seg12));
  EXPECT_TRUE(seg.IsPrefixOf(seg123));
  EXPECT_TRUE(seg.IsPrefixOf(seg2));

  EXPECT_FALSE(seg1.IsPrefixOf(seg));
  EXPECT_TRUE(seg1.IsPrefixOf(seg1));
  EXPECT_TRUE(seg1.IsPrefixOf(seg12));
  EXPECT_TRUE(seg1.IsPrefixOf(seg123));
  EXPECT_FALSE(seg1.IsPrefixOf(seg2));

  EXPECT_FALSE(seg2.IsPrefixOf(seg));
  EXPECT_FALSE(seg2.IsPrefixOf(seg1));
  EXPECT_FALSE(seg2.IsPrefixOf(seg12));
  EXPECT_FALSE(seg2.IsPrefixOf(seg123));
  EXPECT_TRUE(seg2.IsPrefixOf(seg2));

  EXPECT_FALSE(seg12.IsPrefixOf(seg));
  EXPECT_FALSE(seg12.IsPrefixOf(seg1));
  EXPECT_TRUE(seg12.IsPrefixOf(seg12));
  EXPECT_TRUE(seg12.IsPrefixOf(seg123));
  EXPECT_FALSE(seg12.IsPrefixOf(seg2));

  EXPECT_FALSE(seg123.IsPrefixOf(seg));
  EXPECT_FALSE(seg123.IsPrefixOf(seg1));
  EXPECT_FALSE(seg123.IsPrefixOf(seg12));
  EXPECT_TRUE(seg123.IsPrefixOf(seg123));
  EXPECT_FALSE(seg123.IsPrefixOf(seg2));
}

TEST(PathSegment, GetCommonPrefix) {
  PathSegment<16> seg({});
  PathSegment<16> seg1({1});
  PathSegment<16> seg12({1, 2});
  PathSegment<16> seg123({1, 2, 3});
  PathSegment<16> seg2({2});

  EXPECT_EQ(GetCommonPrefix(seg, seg), seg);
  EXPECT_EQ(GetCommonPrefix(seg, seg1), seg);
  EXPECT_EQ(GetCommonPrefix(seg, seg12), seg);
  EXPECT_EQ(GetCommonPrefix(seg, seg123), seg);
  EXPECT_EQ(GetCommonPrefix(seg, seg2), seg);

  EXPECT_EQ(GetCommonPrefix(seg1, seg), seg);
  EXPECT_EQ(GetCommonPrefix(seg1, seg1), seg1);
  EXPECT_EQ(GetCommonPrefix(seg1, seg12), seg1);
  EXPECT_EQ(GetCommonPrefix(seg1, seg123), seg1);
  EXPECT_EQ(GetCommonPrefix(seg1, seg2), seg);

  EXPECT_EQ(GetCommonPrefix(seg12, seg), seg);
  EXPECT_EQ(GetCommonPrefix(seg12, seg1), seg1);
  EXPECT_EQ(GetCommonPrefix(seg12, seg12), seg12);
  EXPECT_EQ(GetCommonPrefix(seg12, seg123), seg12);
  EXPECT_EQ(GetCommonPrefix(seg12, seg2), seg);

  EXPECT_EQ(GetCommonPrefix(seg123, seg), seg);
  EXPECT_EQ(GetCommonPrefix(seg123, seg1), seg1);
  EXPECT_EQ(GetCommonPrefix(seg123, seg12), seg12);
  EXPECT_EQ(GetCommonPrefix(seg123, seg123), seg123);
  EXPECT_EQ(GetCommonPrefix(seg123, seg2), seg);

  EXPECT_EQ(GetCommonPrefix(seg2, seg), seg);
  EXPECT_EQ(GetCommonPrefix(seg2, seg1), seg);
  EXPECT_EQ(GetCommonPrefix(seg2, seg12), seg);
  EXPECT_EQ(GetCommonPrefix(seg2, seg123), seg);
  EXPECT_EQ(GetCommonPrefix(seg2, seg2), seg2);
}

TEST(PathIterator, EnumeratesNibblesInOrder) {
  PathIterator<32> iter(std::bitset<32>(1234567890));
  EXPECT_THAT(PrintToString(iter.GetRemaining()), StrEq("499602d2 : 32"));
  EXPECT_THAT(PrintToString(iter.Next()), StrEq("4"));
  EXPECT_THAT(PrintToString(iter.GetRemaining()), StrEq("099602d2 : 28"));
  EXPECT_THAT(PrintToString(iter.Next()), StrEq("9"));
  EXPECT_THAT(PrintToString(iter.GetRemaining()), StrEq("009602d2 : 24"));
  EXPECT_THAT(PrintToString(iter.Next()), StrEq("9"));
  EXPECT_THAT(PrintToString(iter.GetRemaining()), StrEq("000602d2 : 20"));
  EXPECT_THAT(PrintToString(iter.Next()), StrEq("6"));
  EXPECT_THAT(PrintToString(iter.GetRemaining()), StrEq("000002d2 : 16"));
  EXPECT_THAT(PrintToString(iter.Next()), StrEq("0"));
  EXPECT_THAT(PrintToString(iter.GetRemaining()), StrEq("000002d2 : 12"));
  EXPECT_THAT(PrintToString(iter.Next()), StrEq("2"));
  EXPECT_THAT(PrintToString(iter.GetRemaining()), StrEq("000000d2 : 8"));
  EXPECT_THAT(PrintToString(iter.Next()), StrEq("d"));
  EXPECT_THAT(PrintToString(iter.GetRemaining()), StrEq("00000002 : 4"));
  EXPECT_THAT(PrintToString(iter.Next()), StrEq("2"));
  EXPECT_THAT(PrintToString(iter.GetRemaining()), StrEq("00000000 : 0"));
}

TEST(MerklePatriciaTrie, SetAndRetrieve) {
  MerklePatriciaTrie<std::uint64_t, int> trie;
  EXPECT_THAT(trie.Get(12), Eq(0));
  trie.Set(12, 14);
  EXPECT_THAT(trie.Get(12), Eq(14));
}

TEST(MerklePatriciaTrie, ValuesCanBeUpdated) {
  MerklePatriciaTrie<std::uint64_t, int> trie;
  EXPECT_THAT(trie.Get(12), Eq(0));
  trie.Set(12, 14);
  EXPECT_THAT(trie.Get(12), Eq(14));
  trie.Set(12, 16);
  EXPECT_THAT(trie.Get(12), Eq(16));

  trie.Set(10, 10);
  EXPECT_THAT(trie.Get(12), Eq(16));

  trie.Set(12, 18);
  EXPECT_THAT(trie.Get(12), Eq(18));
}

TEST(MerklePatriciaTrie, SetAndRetrieveMultipleElements) {
  MerklePatriciaTrie<std::uint64_t, int> trie;
  EXPECT_THAT(trie.Get(12), Eq(0));
  EXPECT_THAT(trie.Get(14), Eq(0));
  trie.Set(12, 14);
  trie.Set(1 << 20, 20);
  trie.Set(14, 16);
  EXPECT_THAT(trie.Get(12), Eq(14));
  EXPECT_THAT(trie.Get(14), Eq(16));
  EXPECT_THAT(trie.Get(1 << 20), Eq(20));
}

TEST(MerklePatriciaTrie, RandomInsertAndFind) {
  std::vector<int> data(100);
  std::iota(data.begin(), data.end(), 0);

  std::random_device rd;
  std::mt19937 g(rd());
  std::shuffle(data.begin(), data.end(), g);

  MerklePatriciaTrie<std::uint64_t, int> trie;
  EXPECT_OK(trie.Check());
  for (std::size_t i = 0; i < data.size(); i++) {
    int cur = data[i];
    trie.Set(cur * 101, cur);
    EXPECT_OK(trie.Check());
    for (std::size_t j = 0; j <= i; j++) {
      EXPECT_THAT(trie.Get(data[j] * 101), Eq(data[j]));
    }
    for (std::size_t j = i + 1; j < data.size(); j++) {
      EXPECT_THAT(trie.Get(data[j] * 101), Eq(0));
    }
  }
}

TEST(MerklePatriciaTrie, RandomInsertAndFindWithCollisions) {
  std::vector<int> data(50);
  std::iota(data.begin(), data.end(), 0);
  data.insert(data.end(), data.begin(), data.end());

  std::random_device rd;
  std::mt19937 g(rd());
  std::shuffle(data.begin(), data.end(), g);

  MerklePatriciaTrie<std::uint64_t, int> trie;
  EXPECT_OK(trie.Check());
  for (std::size_t i = 0; i < data.size(); i++) {
    int cur = data[i];
    trie.Set(cur * 101, cur);
    EXPECT_OK(trie.Check());
    for (std::size_t j = 0; j <= i; j++) {
      EXPECT_THAT(trie.Get(data[j] * 101), Eq(data[j]));
    }
  }
}

TEST(MerklePatriciaTrie, RandomInsertAndFindWith256BitKey) {
  constexpr auto N = 100;
  std::random_device rd;
  std::mt19937 g(rd());
  std::uniform_int_distribution distribution(0, 255);

  std::set<Key> keys;
  while (keys.size() < N) {
    Key key;
    for (int i = 0; i < 32; i++) {
      key[i] = distribution(g);
    }
    keys.insert(key);
  }

  std::vector<Key> data(keys.begin(), keys.end());
  std::shuffle(data.begin(), data.end(), g);

  MerklePatriciaTrie<Key, int> trie;
  EXPECT_OK(trie.Check());
  for (std::size_t i = 0; i < data.size(); i++) {
    Key cur = data[i];
    trie.Set(cur, i);
    EXPECT_OK(trie.Check());
    for (std::size_t j = 0; j <= i; j++) {
      EXPECT_THAT(trie.Get(data[j]), Eq(j)) << "Key: " << data[j];
    }
    for (std::size_t j = i + 1; j < data.size(); j++) {
      EXPECT_THAT(trie.Get(data[j]), Eq(0)) << "Key: " << data[j];
    }
  }
}

TEST(MerklePatriciaTrie, RandomDeleteWith256BitKey) {
  constexpr auto N = 100;
  std::random_device rd;
  std::mt19937 g(rd());
  std::uniform_int_distribution distribution(0, 255);

  std::set<Key> keys;
  while (keys.size() < N) {
    Key key;
    for (int i = 0; i < 32; i++) {
      key[i] = distribution(g);
    }
    keys.insert(key);
  }

  std::vector<Key> data(keys.begin(), keys.end());
  std::shuffle(data.begin(), data.end(), g);

  MerklePatriciaTrie<Key, int> trie;
  for (std::size_t i = 0; i < data.size(); i++) {
    trie.Set(data[i], i);
  }
  EXPECT_OK(trie.Check());

  for (std::size_t i = 0; i < data.size(); i++) {
    trie.Set(data[i], 0);
    EXPECT_OK(trie.Check());
    for (std::size_t j = 0; j <= i; j++) {
      EXPECT_THAT(trie.Get(data[j]), Eq(0)) << "Key: " << data[j];
    }
    for (std::size_t j = i + 1; j < data.size(); j++) {
      EXPECT_THAT(trie.Get(data[j]), Eq(j)) << "Key: " << data[j];
    }
  }
}

TEST(MerklePatriciaTrie, ExpansionNodesAreUsed) {
  MerklePatriciaTrie<std::uint64_t, int> trie;
  EXPECT_THAT(trie.Get(12), Eq(0));
  EXPECT_THAT(trie.Get(14), Eq(0));
  trie.Set(12, 14);
  EXPECT_THAT(trie.GetDepth(12), Eq(1));
  trie.Set(1 << 20, 16);
  EXPECT_THAT(trie.GetDepth(12), Eq(3));
}

TEST(MerklePatriciaTrie, DefaultValuesAreNotStored) {
  MerklePatriciaTrie<std::uint64_t, int> trie;
  EXPECT_THAT(trie.GetDepth(12), Eq(0));
  trie.Set(12, 14);
  EXPECT_OK(trie.Check());
  // trie.Dump();
  EXPECT_THAT(trie.GetDepth(12), Eq(1));
  trie.Set(12, 0);
  EXPECT_OK(trie.Check());
  // trie.Dump();
  EXPECT_THAT(trie.GetDepth(12), Eq(0));
}

TEST(MerklePatriciaTrie, DefaultValuesCollapseBranches) {
  MerklePatriciaTrie<std::uint64_t, int> trie;
  EXPECT_THAT(trie.GetDepth(12), Eq(0));
  EXPECT_THAT(trie.GetDepth(1 << 20), Eq(0));
  trie.Set(12, 14);
  trie.Set(1 << 20, 20);
  EXPECT_OK(trie.Check());
  // trie.Dump();
  EXPECT_THAT(trie.GetDepth(12), Eq(3));
  EXPECT_THAT(trie.GetDepth(1 << 20), Eq(3));
  trie.Set(12, 0);
  EXPECT_OK(trie.Check());
  // trie.Dump();
  EXPECT_THAT(trie.GetDepth(12), Eq(1));
  EXPECT_THAT(trie.GetDepth(1 << 20), Eq(1));
}

TEST(MerklePatriciaTrie, RandomDelete) {
  std::vector<int> data(100);
  std::iota(data.begin(), data.end(), 0);

  std::random_device rd;
  std::mt19937 g(rd());
  std::shuffle(data.begin(), data.end(), g);

  MerklePatriciaTrie<std::uint64_t, int> trie;
  for (auto cur : data) {
    trie.Set(cur * 101, cur);
  }
  EXPECT_OK(trie.Check());

  for (std::size_t i = 0; i < data.size(); i++) {
    int cur = data[i];
    // Delete it twice, once present, once absend.
    for (int k = 0; k < 2; k++) {
      trie.Set(cur * 101, 0);
      EXPECT_OK(trie.Check());
      for (std::size_t j = 0; j <= i; j++) {
        EXPECT_THAT(trie.Get(data[j] * 101), Eq(0));
      }
      for (std::size_t j = i + 1; j < data.size(); j++) {
        EXPECT_THAT(trie.Get(data[j] * 101), Eq(data[j]));
      }
    }
  }
}

}  // namespace
}  // namespace carmen::s4
