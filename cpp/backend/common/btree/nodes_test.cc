#include "backend/common/btree/nodes.h"

#include "backend/common/btree/insert_result.h"
#include "backend/common/file.h"
#include "backend/common/page_manager.h"
#include "backend/common/page_pool.h"
#include "common/status_test_util.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::backend::btree {
namespace {

using ::testing::_;
using ::testing::ElementsAre;
using ::testing::IsEmpty;
using ::testing::IsOkAndHolds;
using ::testing::StatusIs;
using ::testing::StrEq;

using TestPagePool = PagePool<InMemoryFile<kFileSystemPageSize>>;

constexpr auto kInternal = absl::StatusCode::kInternal;

// ----------------------------------------------------------------------------
//                              Tests Utils
// ----------------------------------------------------------------------------

// A slightly extended page manager to simplfy test case definitions.
class TestPageManager : public PageManager<TestPagePool> {
 public:
  TestPageManager() : PageManager(TestPagePool()) {}

  // An extension for the test version of the manager to reduce boiler plate
  // code and increase readability by avoiding unnecessary error handling.
  template <Page Node>
  Node& Create() {
    return *New<Node>();
  }
};

// A mock version of a page manager to test the nodes interaction with it.
class MockPageManager : public TestPageManager {
 public:
  MOCK_METHOD(void, MarkAsDirty, (PageId id));
};

// A utility function producing random sequences.
template <typename T>
std::vector<T> GetRandomSequence(std::size_t size) {
  std::vector<T> data;
  for (std::size_t i = 0; i < size; i++) {
    data.push_back(i);
  }
  std::random_device rd;
  std::mt19937 g(rd());
  std::shuffle(data.begin(), data.end(), g);
  return data;
}

// A utility function to create simple trees with two leaf nodes and one inner
// node with a single key.
template <Page Inner>
StatusOrRef<Inner> Create(TestPageManager& manager,
                          std::initializer_list<int> left, int key,
                          std::initializer_list<int> right) {
  using Leaf = typename Inner::leaf_t;
  auto& inner = manager.Create<Inner>();
  ASSIGN_OR_RETURN((auto [left_id, left_leaf]), manager.New<Leaf>());
  ASSIGN_OR_RETURN((auto [right_id, right_leaf]), manager.New<Leaf>());
  for (int cur : left) {
    EXPECT_THAT(left_leaf.Insert(left_id, cur, manager), EntryAdded{});
  }
  for (int cur : right) {
    EXPECT_THAT(right_leaf.Insert(right_id, cur, manager), EntryAdded{});
  }
  inner.Init(left_id, key, right_id);
  return inner;
}

// A utility function to create simple trees with three leaf nodes and one inner
// node with two keys.
template <Page Inner>
StatusOrRef<Inner> Create(TestPageManager& manager,
                          std::initializer_list<int> a, int k1,
                          std::initializer_list<int> b, int k2,
                          std::initializer_list<int> c) {
  ASSIGN_OR_RETURN(Inner & result, Create<Inner>(manager, a, k1, b));
  ASSIGN_OR_RETURN((auto [id, leaf]), manager.New<typename Inner::leaf_t>());
  for (int cur : c) {
    EXPECT_THAT(leaf.Insert(id, cur, manager), EntryAdded{});
  }
  result.Append(k2, id);
  return result;
}

// A utility function to create simple trees with four leaf nodes and one inner
// node with three key.
template <Page Inner>
StatusOrRef<Inner> Create(TestPageManager& manager,
                          std::initializer_list<int> a, int k1,
                          std::initializer_list<int> b, int k2,
                          std::initializer_list<int> c, int k3,
                          std::initializer_list<int> d) {
  ASSIGN_OR_RETURN(Inner & result, Create<Inner>(manager, a, k1, b, k2, c));
  ASSIGN_OR_RETURN((auto [id, leaf]), manager.New<typename Inner::leaf_t>());
  for (int cur : d) {
    EXPECT_THAT(leaf.Insert(id, cur, manager), EntryAdded{});
  }
  result.Append(k3, id);
  return result;
}

// A utilty to define the structure of a tree for test cases. Tree instances of
// this type can be used to create trees using Inner and Leaf nodes of the same
// structure for test cases. Also, Tree structures can be derived from BTree
// node based trees and compared, for defining expectations in tests.
template <std::size_t level>
struct Tree {
  template <Page Inner, typename PageManager>
  Inner& Create(PageManager& manager) const {
    auto id = Build<Inner>(manager);
    return *manager.template Get<Inner>(id);
  }

  template <Page Inner, typename PageManager>
  PageId Build(PageManager& manager) const {
    // Create the inner node first to have page ID 0 for root.
    auto [id, node] = *manager.template New<Inner>();
    std::vector<PageId> sub;
    for (const auto& child : children) {
      sub.push_back(child.template Build<Inner>(manager));
    }
    node.Init(sub[0], keys[0], sub[1]);
    for (std::size_t i = 1; i < keys.size(); i++) {
      node.Append(keys[i], sub[i + 1]);
    }
    return id;
  }

  friend bool operator==(const Tree&, const Tree&) = default;

  friend std::ostream& operator<<(std::ostream& out, const Tree& tree) {
    out << "Node(";
    bool first = true;
    for (std::size_t i = 0; i < tree.keys.size(); i++) {
      if (!first) {
        out << ",";
      } else {
        first = false;
      }
      out << tree.children[i];
      out << "," << tree.keys[i];
    }
    if (!first) out << ",";
    return out << tree.children.back() << ")";
  }

  std::vector<int> keys;
  std::vector<Tree<level - 1>> children;
};

// Specialization of the type above for leaf level nodes.
template <>
struct Tree<0> {
  template <Page Inner, typename PageManager>
  typename Inner::leaf_t& Create(PageManager& manager) const {
    auto id = Build<Inner>(manager);
    return *manager.template Get<typename Inner::leaf_t>(id);
  }

  template <Page Inner, typename PageManager>
  PageId Build(PageManager& manager) const {
    auto [id, node] = *manager.template New<typename Inner::leaf_t>();
    using entry_t = typename Inner::leaf_t::entry_t;
    std::vector<entry_t> entries;
    for (auto cur : values) {
      if constexpr (std::is_same_v<entry_t, Entry<int, int>>) {
        entries.push_back(entry_t(cur, cur));
      } else {
        entries.push_back(entry_t(cur));
      }
    }
    node.SetTestEntries(entries);
    return id;
  }

  friend bool operator==(const Tree&, const Tree&) = default;

  friend std::ostream& operator<<(std::ostream& out, const Tree& tree) {
    out << "Node(";
    bool first = true;
    for (const auto& cur : tree.values) {
      if (!first) {
        out << ",";
      } else {
        first = false;
      }
      out << cur;
    }
    return out << ")";
  }

  std::vector<int> values;
};

// The following functions define factories to build aribtray tree sturctures
// using nested Node(..) invocations. For instance, by calling
//
//    Node(Node(1,2),3,Node(4,5,6),7,Node(8))
//
// a tree with an inner node with two keys (3 and 7) is created, together with
// three leaf nodes, containing the elements (1,2), (4,5,6), and (8)
// respectively. Note that those trees are not bound to any ordering or size
// constraints, and may thus be used to create degenerated/illformed instances.

// A factory for leaf-level Trees.
template <typename... Ints>
requires(std::same_as<Ints, int>&&...) Tree<0> Node(Ints&&... args) {
  return Tree<0>{std::vector<int>{args...}};
}

// A factory for non-leaf-level Trees.
template <std::size_t level>
Tree<level + 1> Node(Tree<level> a, int k1, Tree<level> b) {
  return Tree<level + 1>{std::vector<int>{k1}, std::vector<Tree<level>>{a, b}};
}

template <std::size_t level>
Tree<level> Extend(Tree<level> tree) {
  return tree;
}

template <std::size_t level, typename... Args>
Tree<level> Extend(Tree<level> tree, int key, Tree<level - 1> child,
                   Args&&... args) {
  tree.keys.push_back(key);
  tree.children.push_back(std::move(child));
  return Extend(tree, std::forward<Args>(args)...);
}

template <std::size_t level, typename... Args>
Tree<level + 1> Node(Tree<level> a, int k1, Tree<level> b, Args&&... args) {
  auto res = Node(a, k1, b);
  return Extend(res, std::forward<Args>(args)...);
}

// Can be used to create a BTree leaf node matching the given tree structure.
template <typename Leaf, typename PageManager>
Leaf& Create(PageManager& manager, const Tree<0>& tree) {
  return tree.template Create<InnerNode<Leaf>>(manager);
}

// Can be used to create a BTree sub-tree matching the given tree structure.
template <typename Inner, typename PageManager, std::size_t level>
Inner& Create(PageManager& manager, const Tree<level>& tree) {
  return tree.template Create<Inner>(manager);
}

// A utility allowing to converte the split result of a insertion operation into
// a new root node. This is frequently required in tests, and thus factored out
// here. It assumes that the previous root had page_id == 0.
template <typename Inner, typename PageManager>
Inner& CreateNewRoot(PageManager& manager, InsertResult<int> insert_result) {
  EXPECT_TRUE(std::holds_alternative<Split<int>>(insert_result));
  auto& split = std::get<Split<int>>(insert_result);
  Inner& result = manager.template Create<Inner>();
  result.Init(0, split.key, split.new_tree);
  return result;
}

// Converts a BTree sub-tree into a Tree structure.
template <std::size_t level, typename Node, typename PageManager>
Tree<level> ToTree(const Node& node, PageManager& manager) {
  if constexpr (level == 0) {
    Tree<0> result;
    for (auto cur : node.GetEntries()) {
      result.values.push_back(cur.key);
    }
    return result;
  } else {
    using Inner = Node;
    using Leaf = typename Node::leaf_t;
    Tree<level> result;
    for (int key : node.GetKeys()) {
      result.keys.push_back(key);
    }
    for (PageId child : node.GetChildren()) {
      if constexpr (level > 1) {
        Inner& node = *manager.template Get<Inner>(child);
        result.children.push_back(ToTree<level - 1>(node, manager));
      } else {
        Leaf& leaf = *manager.template Get<Leaf>(child);
        result.children.push_back(ToTree<0>(leaf, manager));
      }
    }
    return result;
  }
}

// ----------------------------------------------------------------------------
//                                 Tests
// ----------------------------------------------------------------------------

TEST(LeafNode, IsPage) {
  EXPECT_TRUE((Page<LeafNode<int, Unit>>));
  EXPECT_EQ(sizeof(LeafNode<int, Unit>), kFileSystemPageSize);

  EXPECT_TRUE((Page<LeafNode<int, int>>));
  EXPECT_EQ(sizeof(LeafNode<int, int>), kFileSystemPageSize);

  EXPECT_TRUE((Page<LeafNode<int, Unit, std::less<int>, 4>>));
  EXPECT_EQ(sizeof(LeafNode<int, Unit, std::less<int>, 4>),
            kFileSystemPageSize);

  EXPECT_TRUE((Page<LeafNode<int, double, std::less<int>, 4>>));
  EXPECT_EQ(sizeof(LeafNode<int, double, std::less<int>, 4>),
            kFileSystemPageSize);

  EXPECT_TRUE((Page<LeafNode<Value, Unit>>));
  EXPECT_EQ(sizeof(LeafNode<Value, Unit>), kFileSystemPageSize);

  EXPECT_TRUE((Page<LeafNode<Value, Unit, std::less<Value>, 4>>));
  EXPECT_EQ(sizeof(LeafNode<Value, Unit, std::less<Value>, 4>),
            kFileSystemPageSize);
}

TEST(LeafNode, DefaultMaxElementsUsesFullNodeSize) {
  EXPECT_EQ(
      (LeafNode<std::uint8_t, Unit>::kMaxEntries),
      (kFileSystemPageSize - sizeof(std::uint16_t)) / sizeof(std::uint8_t));
  EXPECT_EQ(
      (LeafNode<std::uint64_t, Unit>::kMaxEntries),
      (kFileSystemPageSize - sizeof(std::uint16_t)) / sizeof(std::uint64_t));

  EXPECT_EQ((LeafNode<std::uint8_t, std::uint16_t>::kMaxEntries),
            (kFileSystemPageSize - sizeof(std::uint16_t)) /
                (sizeof(std::uint8_t) + sizeof(std::uint16_t)));
}

TEST(LeafNode, ZeroInitializedNodeIsEmpty) {
  TestPageManager manager;
  auto& leaf = manager.Create<LeafNode<int>>();
  EXPECT_THAT(leaf.GetEntries(), IsEmpty());
}

TEST(LeafNode, InsertedElementsAreOrdered) {
  using Leaf = LeafNode<int>;
  TestPageManager manager;
  ASSERT_GT(Leaf::kMaxEntries, 5);

  auto& leaf = manager.Create<Leaf>();
  EXPECT_THAT(leaf.GetEntries(), IsEmpty());

  EXPECT_THAT(leaf.Insert(0, 2, manager), EntryAdded{});
  EXPECT_THAT(leaf.GetEntries(), ElementsAre(2));

  EXPECT_THAT(leaf.Insert(0, 1, manager), EntryAdded{});
  EXPECT_THAT(leaf.GetEntries(), ElementsAre(1, 2));

  EXPECT_THAT(leaf.Insert(0, 4, manager), EntryAdded{});
  EXPECT_THAT(leaf.GetEntries(), ElementsAre(1, 2, 4));

  EXPECT_THAT(leaf.Insert(0, 3, manager), EntryAdded{});
  EXPECT_THAT(leaf.GetEntries(), ElementsAre(1, 2, 3, 4));
}

TEST(LeafNode, InsertionOrderCanBeCustomized) {
  using Leaf = LeafNode<int, Unit, std::greater<int>>;
  TestPageManager manager;
  ASSERT_GT(Leaf::kMaxEntries, 5);

  auto& leaf = manager.Create<Leaf>();
  EXPECT_THAT(leaf.GetEntries(), IsEmpty());

  EXPECT_THAT(leaf.Insert(0, 2, manager), EntryAdded{});
  EXPECT_THAT(leaf.GetEntries(), ElementsAre(2));

  EXPECT_THAT(leaf.Insert(0, 1, manager), EntryAdded{});
  EXPECT_THAT(leaf.GetEntries(), ElementsAre(2, 1));

  EXPECT_THAT(leaf.Insert(0, 4, manager), EntryAdded{});
  EXPECT_THAT(leaf.GetEntries(), ElementsAre(4, 2, 1));

  EXPECT_THAT(leaf.Insert(0, 3, manager), EntryAdded{});
  EXPECT_THAT(leaf.GetEntries(), ElementsAre(4, 3, 2, 1));
}

TEST(LeafNode, DuplicateElementsAreIgnored) {
  using Leaf = LeafNode<int>;
  TestPageManager manager;
  ASSERT_GT(Leaf::kMaxEntries, 5);

  auto& leaf = manager.Create<Leaf>();
  EXPECT_THAT(leaf.GetEntries(), IsEmpty());

  EXPECT_THAT(leaf.Insert(0, 1, manager), EntryAdded{});
  EXPECT_THAT(leaf.Insert(0, 2, manager), EntryAdded{});
  EXPECT_THAT(leaf.GetEntries(), ElementsAre(1, 2));

  EXPECT_THAT(leaf.Insert(0, 1, manager), EntryPresent{});
  EXPECT_THAT(leaf.GetEntries(), ElementsAre(1, 2));

  EXPECT_THAT(leaf.Insert(0, 2, manager), EntryPresent{});
  EXPECT_THAT(leaf.GetEntries(), ElementsAre(1, 2));
}

TEST(LeafNode, InsertionTriggersSplitIfTooFull) {
  using Leaf = LeafNode<int, Unit, std::less<int>, 4>;
  TestPageManager manager;
  ASSERT_EQ(Leaf::kMaxEntries, 4);

  auto& leaf = manager.Create<Leaf>();

  // Fill the leaf to the limit.
  EXPECT_THAT(leaf.Insert(0, 1, manager), EntryAdded{});
  EXPECT_THAT(leaf.Insert(0, 2, manager), EntryAdded{});
  EXPECT_THAT(leaf.Insert(0, 3, manager), EntryAdded{});
  EXPECT_THAT(leaf.Insert(0, 4, manager), EntryAdded{});
  EXPECT_THAT(leaf.GetEntries(), ElementsAre(1, 2, 3, 4));

  // The next element triggers a split.
  EXPECT_THAT(leaf.Insert(0, 5, manager), (Split<int>{3, PageId(1)}));
  EXPECT_THAT(leaf.GetEntries(), ElementsAre(1, 2));
  ASSERT_OK_AND_ASSIGN(Leaf & overflow, manager.Get<Leaf>(1));
  EXPECT_THAT(overflow.GetEntries(), ElementsAre(4, 5));
}

TEST(LeafNode, SplitWithElementOnTheRightIsBalanced) {
  using Leaf = LeafNode<int, Unit, std::less<int>, 4>;
  TestPageManager manager;
  ASSERT_EQ(Leaf::kMaxEntries, 4);

  auto& leaf = manager.Create<Leaf>();

  // Fill the leaf to the limit.
  EXPECT_THAT(leaf.Insert(0, 1, manager), EntryAdded{});
  EXPECT_THAT(leaf.Insert(0, 3, manager), EntryAdded{});
  EXPECT_THAT(leaf.Insert(0, 4, manager), EntryAdded{});
  EXPECT_THAT(leaf.Insert(0, 5, manager), EntryAdded{});
  EXPECT_THAT(leaf.GetEntries(), ElementsAre(1, 3, 4, 5));

  // Trigger the split with an element that should end up on the left.
  EXPECT_THAT(leaf.Insert(0, 2, manager), (Split<int>{3, PageId(1)}));
  EXPECT_THAT(leaf.GetEntries(), ElementsAre(1, 2));
  ASSERT_OK_AND_ASSIGN(Leaf & overflow, manager.Get<Leaf>(1));
  EXPECT_THAT(overflow.GetEntries(), ElementsAre(4, 5));
}

TEST(LeafNode, NewElementCanBeTheSplitKey) {
  using Leaf = LeafNode<int, Unit, std::less<int>, 4>;
  TestPageManager manager;
  ASSERT_EQ(Leaf::kMaxEntries, 4);

  auto& leaf = manager.Create<Leaf>();

  // Fill the leaf to the limit.
  EXPECT_THAT(leaf.Insert(0, 1, manager), EntryAdded{});
  EXPECT_THAT(leaf.Insert(0, 2, manager), EntryAdded{});
  EXPECT_THAT(leaf.Insert(0, 4, manager), EntryAdded{});
  EXPECT_THAT(leaf.Insert(0, 5, manager), EntryAdded{});
  EXPECT_THAT(leaf.GetEntries(), ElementsAre(1, 2, 4, 5));

  // Trigger the split with an element that should end up on the left.
  EXPECT_THAT(leaf.Insert(0, 3, manager), (Split<int>{3, PageId(1)}));
  EXPECT_THAT(leaf.GetEntries(), ElementsAre(1, 2));
  ASSERT_OK_AND_ASSIGN(Leaf & overflow, manager.Get<Leaf>(1));
  EXPECT_THAT(overflow.GetEntries(), ElementsAre(4, 5));
}

TEST(LeafNode, SplittingOddCapacityNodeLeadsToLargerLeftNode_InsertLeft) {
  using Leaf = LeafNode<int, Unit, std::less<int>, 3>;
  TestPageManager manager;
  ASSERT_EQ(Leaf::kMaxEntries, 3);

  auto& leaf = manager.Create<Leaf>();

  // Fill the leaf to the limit.
  EXPECT_THAT(leaf.Insert(0, 1, manager), EntryAdded{});
  EXPECT_THAT(leaf.Insert(0, 3, manager), EntryAdded{});
  EXPECT_THAT(leaf.Insert(0, 4, manager), EntryAdded{});
  EXPECT_THAT(leaf.GetEntries(), ElementsAre(1, 3, 4));

  // Trigger the split with an element that should end up on the left.
  EXPECT_THAT(leaf.Insert(0, 2, manager), (Split<int>{3, PageId(1)}));
  EXPECT_THAT(leaf.GetEntries(), ElementsAre(1, 2));
  ASSERT_OK_AND_ASSIGN(Leaf & overflow, manager.Get<Leaf>(1));
  EXPECT_THAT(overflow.GetEntries(), ElementsAre(4));
}

TEST(LeafNode, SplittingOddCapacityNodeLeadsToLargerLeftNode_InsertRight) {
  using Leaf = LeafNode<int, Unit, std::less<int>, 3>;
  TestPageManager manager;
  ASSERT_EQ(Leaf::kMaxEntries, 3);

  auto& leaf = manager.Create<Leaf>();

  // Fill the leaf to the limit.
  EXPECT_THAT(leaf.Insert(0, 1, manager), EntryAdded{});
  EXPECT_THAT(leaf.Insert(0, 2, manager), EntryAdded{});
  EXPECT_THAT(leaf.Insert(0, 3, manager), EntryAdded{});
  EXPECT_THAT(leaf.GetEntries(), ElementsAre(1, 2, 3));

  // Trigger the split with an element that should end up on the left.
  EXPECT_THAT(leaf.Insert(0, 4, manager), (Split<int>{3, PageId(1)}));
  EXPECT_THAT(leaf.GetEntries(), ElementsAre(1, 2));
  ASSERT_OK_AND_ASSIGN(Leaf & overflow, manager.Get<Leaf>(1));
  EXPECT_THAT(overflow.GetEntries(), ElementsAre(4));
}

TEST(LeafNode, SplittingOddCapacityNodeLeadsToLargerLeftNode_InsertCenter) {
  using Leaf = LeafNode<int, Unit, std::less<int>, 3>;
  TestPageManager manager;
  ASSERT_EQ(Leaf::kMaxEntries, 3);

  auto& leaf = manager.Create<Leaf>();

  // Fill the leaf to the limit.
  EXPECT_THAT(leaf.Insert(0, 1, manager), EntryAdded{});
  EXPECT_THAT(leaf.Insert(0, 2, manager), EntryAdded{});
  EXPECT_THAT(leaf.Insert(0, 4, manager), EntryAdded{});
  EXPECT_THAT(leaf.GetEntries(), ElementsAre(1, 2, 4));

  // Trigger the split with an element that should end up on the left.
  EXPECT_THAT(leaf.Insert(0, 3, manager), (Split<int>{3, PageId(1)}));
  EXPECT_THAT(leaf.GetEntries(), ElementsAre(1, 2));
  ASSERT_OK_AND_ASSIGN(Leaf & overflow, manager.Get<Leaf>(1));
  EXPECT_THAT(overflow.GetEntries(), ElementsAre(4));
}

TEST(LeafNode, SplittingMapLeafsKeepsCopyOfKeyInLeaf) {
  using Leaf = LeafNode<int, int, std::less<int>, 4>;
  using E = typename Leaf::entry_t;
  TestPageManager manager;
  ASSERT_EQ(Leaf::kMaxEntries, 4);

  // Create a full map leaf.
  auto& leaf = Create<Leaf>(manager, Node(1, 2, 4, 5));
  EXPECT_THAT(leaf.GetEntries(),
              ElementsAre(E(1, 1), E(2, 2), E(4, 4), E(5, 5)));

  // The next element triggers a split, but all keys remain in leafs.
  EXPECT_THAT(leaf.Insert(0, E(3, 3), manager), (Split<int>{4, PageId(1)}));
  EXPECT_THAT(leaf.GetEntries(), ElementsAre(E(1, 1), E(2, 2), E(3, 3)));
  ASSERT_OK_AND_ASSIGN(Leaf & overflow, manager.Get<Leaf>(1));
  EXPECT_THAT(overflow.GetEntries(), ElementsAre(E(4, 4), E(5, 5)));
}

TEST(LeafNode, SplittingMapLeafsRemainsBalancedForEvenLength) {
  using Leaf = LeafNode<int, int, std::less<int>, 4>;
  using E = typename Leaf::entry_t;
  for (int i = 1; i <= 5; i++) {
    TestPageManager manager;
    ASSERT_EQ(Leaf::kMaxEntries, 4);

    // Fill the leaf to the limit with all elements except i.
    auto& leaf = manager.Create<Leaf>();
    for (int j = 1; j <= 5; j++) {
      if (i != j) {
        EXPECT_THAT(leaf.Insert(0, E(j, j), manager), EntryAdded{});
      }
    }

    // Inserting i triggers the split, the result should always be the same.
    EXPECT_THAT(leaf.Insert(0, E(i, i), manager), (Split<int>{4, PageId(1)}));
    EXPECT_THAT(leaf.GetEntries(), ElementsAre(E(1, 1), E(2, 2), E(3, 3)));
    ASSERT_OK_AND_ASSIGN(Leaf & overflow, manager.Get<Leaf>(1));
    EXPECT_THAT(overflow.GetEntries(), ElementsAre(E(4, 4), E(5, 5)));
  }
}

TEST(LeafNode, InsertingNewElementMarksPageAsDirty) {
  using Leaf = LeafNode<int, Unit, std::less<int>, 3>;
  MockPageManager manager;
  auto& leaf = Create<Leaf>(manager, Node());

  EXPECT_CALL(manager, MarkAsDirty(12)).Times(2);  // once per insert

  EXPECT_THAT(leaf.Insert(12, 1, manager), EntryAdded{});
  EXPECT_THAT(leaf.Insert(12, 2, manager), EntryAdded{});
}

TEST(LeafNode, InsertingExistingElementDoesNotMarkNodeAsDirty) {
  using Leaf = LeafNode<int, Unit, std::less<int>, 3>;
  MockPageManager manager;
  auto& leaf = Create<Leaf>(manager, Node(1));

  EXPECT_CALL(manager, MarkAsDirty(12)).Times(0);  // No call expected.
  EXPECT_THAT(leaf.Insert(12, 1, manager), EntryPresent{});
}

TEST(LeafNode, SplitMarksOldAndNewNodeDirty) {
  using Leaf = LeafNode<int, Unit, std::less<int>, 2>;
  testing::NiceMock<MockPageManager> manager;
  auto& leaf = Create<Leaf>(manager, Node(1, 2));

  EXPECT_CALL(manager, MarkAsDirty(12));
  EXPECT_CALL(manager, MarkAsDirty(1));  // 1 is the ID of the new node.
  EXPECT_THAT(leaf.Insert(12, 3, manager), (Split<int>{2, PageId(1)}));
}

TEST(LeafNode, ContainsFindsPresentElements) {
  using Leaf = LeafNode<int>;
  auto data = GetRandomSequence<int>(Leaf::kMaxEntries);

  TestPageManager manager;
  auto& leaf = manager.Create<Leaf>();
  for (std::size_t i = 0; i < data.size(); i++) {
    for (std::size_t j = 0; j < data.size(); j++) {
      EXPECT_THAT(leaf.Contains(data[j]), j < i);
    }
    EXPECT_THAT(leaf.Insert(0, data[i], manager), EntryAdded{});
    for (std::size_t j = 0; j < data.size(); j++) {
      EXPECT_THAT(leaf.Contains(data[j]), j <= i);
    }
  }
}

TEST(LeafNode, CheckAcceptsAnyNumberOfElementsInRoot) {
  using Leaf = LeafNode<int, Unit, std::less<int>, 4>;
  TestPageManager manager;
  auto& leaf = manager.Create<Leaf>();

  EXPECT_OK(leaf.Check(nullptr, nullptr));
  EXPECT_OK(leaf.Insert(0, 1, manager));
  EXPECT_OK(leaf.Check(nullptr, nullptr));
  EXPECT_OK(leaf.Insert(0, 2, manager));
  EXPECT_OK(leaf.Check(nullptr, nullptr));
  EXPECT_OK(leaf.Insert(0, 3, manager));
  EXPECT_OK(leaf.Check(nullptr, nullptr));
}

TEST(LeafNode, CheckDetectsTooFewElementsForInnerNodes) {
  using Leaf = LeafNode<int, Unit, std::less<int>, 4>;
  TestPageManager manager;
  auto& leaf = manager.Create<Leaf>();

  const int low = 0;
  const int high = 10;
  EXPECT_THAT(
      leaf.Check(&low, &high),
      StatusIs(kInternal,
               StrEq("Invalid number of entries, expected at least 2, got 0")));

  EXPECT_OK(leaf.Insert(0, 1, manager));
  EXPECT_THAT(
      leaf.Check(&low, &high),
      StatusIs(kInternal,
               StrEq("Invalid number of entries, expected at least 2, got 1")));

  EXPECT_OK(leaf.Insert(0, 2, manager));
  EXPECT_OK(leaf.Check(&low, &high));
}

TEST(LeafNode, OrderInconsistenciesAreDetected) {
  using Leaf = LeafNode<int, Unit, std::less<int>>;
  TestPageManager manager;
  auto& leaf = Create<Leaf>(manager, Node(1, 3, 2, 4));
  ASSERT_THAT(leaf.GetEntries(), ElementsAre(1, 3, 2, 4));
  EXPECT_THAT(leaf.Check(nullptr, nullptr),
              StatusIs(kInternal, StrEq("Invalid order of entries")));
}

TEST(LeafNode, DuplicatedElementsAreDetectedAsOrderInconsistencies) {
  using Leaf = LeafNode<int, Unit, std::less<int>>;
  TestPageManager manager;
  auto& leaf = Create<Leaf>(manager, Node(1, 2, 2, 4));
  ASSERT_THAT(leaf.GetEntries(), ElementsAre(1, 2, 2, 4));
  EXPECT_THAT(leaf.Check(nullptr, nullptr),
              StatusIs(kInternal, StrEq("Invalid order of entries")));
}

TEST(LeafNode, BoundViolationsAreDetected) {
  using Leaf = LeafNode<int, Unit, std::less<int>, 4>;
  TestPageManager manager;
  auto& leaf = manager.Create<Leaf>();

  EXPECT_OK(leaf.Insert(0, 1, manager));
  EXPECT_OK(leaf.Insert(0, 2, manager));
  EXPECT_OK(leaf.Insert(0, 3, manager));
  EXPECT_OK(leaf.Check(nullptr, nullptr));

  int limit = 0;
  EXPECT_OK(leaf.Check(&limit, nullptr));
  limit = 1;
  EXPECT_THAT(
      leaf.Check(&limit, nullptr),
      StatusIs(_, StrEq("Lower boundary is not less than smallest entry")));
  limit = 3;
  EXPECT_THAT(
      leaf.Check(nullptr, &limit),
      StatusIs(_, StrEq("Biggest entry is not less than upper boundary")));
  limit = 4;
  EXPECT_OK(leaf.Check(nullptr, &limit));
}

TEST(InnerNode, IsPage) {
  using IntLeaf = LeafNode<int>;
  EXPECT_TRUE(Page<InnerNode<IntLeaf>>);
  EXPECT_EQ(sizeof(InnerNode<IntLeaf>), kFileSystemPageSize);

  EXPECT_TRUE((Page<InnerNode<IntLeaf, 4>>));
  EXPECT_EQ(sizeof(InnerNode<IntLeaf, 4>), kFileSystemPageSize);

  using ValueLeaf = LeafNode<Value>;
  EXPECT_TRUE(Page<InnerNode<ValueLeaf>>);
  EXPECT_EQ(sizeof(InnerNode<ValueLeaf>), kFileSystemPageSize);

  EXPECT_TRUE((Page<InnerNode<ValueLeaf, 4>>));
  EXPECT_EQ(sizeof(InnerNode<ValueLeaf, 4>), kFileSystemPageSize);
}

TEST(InnerNode, CapacityFillsFullNode) {
  EXPECT_EQ(InnerNode<LeafNode<std::uint8_t>>::kMaxKeys,
            (kFileSystemPageSize - sizeof(PageId)) /
                (sizeof(std::uint8_t) + sizeof(PageId)));
  EXPECT_EQ(InnerNode<LeafNode<std::uint64_t>>::kMaxKeys,
            (kFileSystemPageSize - sizeof(PageId)) /
                (sizeof(std::uint64_t) + sizeof(PageId)));
}

TEST(TestInfrastructure, TreeStructureUtilsWork) {
  using Leaf = LeafNode<int, Unit, std::less<int>, 4>;
  using Inner = InnerNode<Leaf, 4>;
  TestPageManager manager;

  auto structure = Node(Node(Node(1, 3), 5, Node(7, 9)), 10,
                        Node(Node(11, 12), 14, Node(15, 16), 17, Node(18, 19)));

  Inner& node = Create<Inner>(manager, structure);
  EXPECT_EQ(ToTree<2>(node, manager), structure);
}

TEST(InnerNode, InsertingElementsToTheLeftChildWorks) {
  using Leaf = LeafNode<int, Unit, std::less<int>, 4>;
  using Inner = InnerNode<Leaf, 4>;
  TestPageManager manager;
  ASSERT_OK_AND_ASSIGN(Inner & node, Create<Inner>(manager, {1, 3}, 5, {7, 9}));
  ASSERT_OK_AND_ASSIGN(Leaf & left, manager.Get<Leaf>(node.GetChildren()[0]));
  ASSERT_OK_AND_ASSIGN(Leaf & right, manager.Get<Leaf>(node.GetChildren()[1]));
  EXPECT_THAT(node.Insert(0, 1, 2, manager), EntryAdded{});
  EXPECT_THAT(left.GetEntries(), ElementsAre(1, 2, 3));
  EXPECT_THAT(right.GetEntries(), ElementsAre(7, 9));
}

TEST(InnerNode, InsertingElementsToTheRightChildWorks) {
  using Leaf = LeafNode<int, Unit, std::less<int>, 4>;
  using Inner = InnerNode<Leaf, 4>;
  TestPageManager manager;
  ASSERT_OK_AND_ASSIGN(Inner & node, Create<Inner>(manager, {1, 3}, 5, {7, 9}));
  ASSERT_OK_AND_ASSIGN(Leaf & left, manager.Get<Leaf>(node.GetChildren()[0]));
  ASSERT_OK_AND_ASSIGN(Leaf & right, manager.Get<Leaf>(node.GetChildren()[1]));
  EXPECT_THAT(node.Insert(0, 1, 8, manager), EntryAdded{});
  EXPECT_THAT(left.GetEntries(), ElementsAre(1, 3));
  EXPECT_THAT(right.GetEntries(), ElementsAre(7, 8, 9));
}

TEST(InnerNode, InsertingElementsPresentInLeftChildIsDetected) {
  using Leaf = LeafNode<int, Unit, std::less<int>, 4>;
  using Inner = InnerNode<Leaf, 4>;
  TestPageManager manager;
  ASSERT_OK_AND_ASSIGN(Inner & node, Create<Inner>(manager, {1, 3}, 5, {7, 9}));
  EXPECT_THAT(node.Insert(0, 1, 3, manager), EntryPresent{});
}

TEST(InnerNode, InsertingElementsPresentAsKeyIsDetected) {
  using Leaf = LeafNode<int, Unit, std::less<int>, 4>;
  using Inner = InnerNode<Leaf, 4>;
  TestPageManager manager;
  ASSERT_OK_AND_ASSIGN(Inner & node, Create<Inner>(manager, {1, 3}, 5, {7, 9}));
  EXPECT_THAT(node.Insert(0, 1, 5, manager), EntryPresent{});
}

TEST(InnerNode, InsertingElementsPresentInRightChildIsDetected) {
  using Leaf = LeafNode<int, Unit, std::less<int>, 4>;
  using Inner = InnerNode<Leaf, 4>;
  TestPageManager manager;
  ASSERT_OK_AND_ASSIGN(Inner & node, Create<Inner>(manager, {1, 3}, 5, {7, 9}));
  EXPECT_THAT(node.Insert(0, 1, 7, manager), EntryPresent{});
}

TEST(InnerNode, LeftNodeSplitExtendsInnerNode) {
  using Leaf = LeafNode<int, Unit, std::less<int>, 4>;
  using Inner = InnerNode<Leaf, 4>;
  TestPageManager manager;
  ASSERT_OK_AND_ASSIGN(Inner & node,
                       Create<Inner>(manager, {1, 2, 3, 5}, 6, {7, 8}));

  // The inner node gets a new entry.
  EXPECT_THAT(node.Insert(0, 1, 4, manager), EntryAdded{});
  EXPECT_THAT(node.GetKeys(), ElementsAre(3, 6));
  ASSERT_THAT(node.GetChildren(), ElementsAre(PageId(1), PageId(3), PageId(2)));

  // The leaf nodes are properly split.
  ASSERT_OK_AND_ASSIGN(Leaf & a, manager.Get<Leaf>(PageId(1)));
  EXPECT_THAT(a.GetEntries(), ElementsAre(1, 2));
  ASSERT_OK_AND_ASSIGN(Leaf & b, manager.Get<Leaf>(PageId(3)));
  EXPECT_THAT(b.GetEntries(), ElementsAre(4, 5));
  ASSERT_OK_AND_ASSIGN(Leaf & c, manager.Get<Leaf>(PageId(2)));
  EXPECT_THAT(c.GetEntries(), ElementsAre(7, 8));
}

TEST(InnerNode, RightNodeSplitExtendsInnerNode) {
  using Leaf = LeafNode<int, Unit, std::less<int>, 4>;
  using Inner = InnerNode<Leaf, 4>;
  TestPageManager manager;
  ASSERT_OK_AND_ASSIGN(Inner & node,
                       Create<Inner>(manager, {1, 2}, 3, {4, 5, 7, 8}));

  // The inner node gets a new entry.
  EXPECT_THAT(node.Insert(0, 1, 6, manager), EntryAdded{});
  EXPECT_THAT(node.GetKeys(), ElementsAre(3, 6));
  ASSERT_THAT(node.GetChildren(), ElementsAre(PageId(1), PageId(2), PageId(3)));

  // The leaf nodes are properly split.
  ASSERT_OK_AND_ASSIGN(Leaf & a, manager.Get<Leaf>(PageId(1)));
  EXPECT_THAT(a.GetEntries(), ElementsAre(1, 2));
  ASSERT_OK_AND_ASSIGN(Leaf & b, manager.Get<Leaf>(PageId(2)));
  EXPECT_THAT(b.GetEntries(), ElementsAre(4, 5));
  ASSERT_OK_AND_ASSIGN(Leaf & c, manager.Get<Leaf>(PageId(3)));
  EXPECT_THAT(c.GetEntries(), ElementsAre(7, 8));
}

TEST(InnerNode, FullInnerNodeIsSplit) {
  using Leaf = LeafNode<int, Unit, std::less<int>, 2>;
  using Inner = InnerNode<Leaf, 2>;
  TestPageManager manager;
  ASSERT_OK_AND_ASSIGN(Inner & node,
                       Create<Inner>(manager, {1, 2}, 4, {5, 6}, 7, {8, 9}));

  // Inserting on the left side splits the inner node.
  ASSERT_OK_AND_ASSIGN(auto insert_result, node.Insert(0, 1, 3, manager));
  EXPECT_THAT(insert_result, (Split<int>{4, PageId(5)}));

  // Create a new root to have a full tree.
  auto& root = CreateNewRoot<Inner>(manager, insert_result);

  // Check the structure of the new tree.
  EXPECT_EQ(ToTree<2>(root, manager), Node(Node(Node(1), 2, Node(3)), 4,
                                           Node(Node(5, 6), 7, Node(8, 9))));

  // Check also that the pages are properly reused. The old root is reduced to
  // the key 2 and two sub-trees, one of those is the new subtree 4.
  EXPECT_THAT(node.GetKeys(), ElementsAre(2));
  EXPECT_THAT(node.GetChildren(), ElementsAre(1, 4));

  // The new sibling node has the remaining key 7 and the previous child pages.
  ASSERT_OK_AND_ASSIGN(Inner & sibling, manager.Get<Inner>(PageId(5)));
  EXPECT_THAT(sibling.GetKeys(), ElementsAre(7));
  EXPECT_THAT(sibling.GetChildren(), ElementsAre(2, 3));
}

TEST(InnerNode, FullInnerNodeSplitByMiddleElementUsesNewElementAsKey) {
  using Leaf = LeafNode<int, Unit, std::less<int>, 2>;
  using Inner = InnerNode<Leaf, 2>;
  TestPageManager manager;
  ASSERT_OK_AND_ASSIGN(Inner & node,
                       Create<Inner>(manager, {1, 2}, 3, {4, 6}, 7, {8, 9}));

  // Inserting element 5 makes 5 the new root key.
  ASSERT_OK_AND_ASSIGN(auto insert_result, node.Insert(0, 1, 5, manager));
  EXPECT_THAT(insert_result, (Split<int>{5, PageId(5)}));

  // Check the structure of the new tree.
  auto& root = CreateNewRoot<Inner>(manager, insert_result);
  EXPECT_EQ(ToTree<2>(root, manager), Node(Node(Node(1, 2), 3, Node(4)), 5,
                                           Node(Node(6), 7, Node(8, 9))));
}

TEST(InnerNode, InsertOnRightSubTreeCausingSplitWorks) {
  using Leaf = LeafNode<int, Unit, std::less<int>, 2>;
  using Inner = InnerNode<Leaf, 2>;
  TestPageManager manager;
  ASSERT_OK_AND_ASSIGN(Inner & node,
                       Create<Inner>(manager, {1, 2}, 3, {4, 5}, 6, {8, 9}));

  // Inserting element 7 ends up splitting the right nodes.
  ASSERT_OK_AND_ASSIGN(auto insert_result, node.Insert(0, 1, 7, manager));
  EXPECT_THAT(insert_result, (Split<int>{6, PageId(5)}));

  // Check the structure of the new tree.
  auto& root = CreateNewRoot<Inner>(manager, insert_result);
  EXPECT_EQ(ToTree<2>(root, manager), Node(Node(Node(1, 2), 3, Node(4, 5)), 6,
                                           Node(Node(7), 8, Node(9))));
}

TEST(InnerNode, SplitInLeftHalfKeepsSiblingsBalanced) {
  using Leaf = LeafNode<int, Unit, std::less<int>, 2>;
  using Inner = InnerNode<Leaf, 4>;
  TestPageManager manager;

  // Create a full tree.
  Inner& node =
      Create<Inner>(manager, Node(Node(1, 2), 3, Node(4, 5), 6, Node(7, 8), 9,
                                  Node(10, 11), 12, Node(13, 14)));

  // Inserting element 0 ends leads to equal balanced inner nodes.
  ASSERT_OK_AND_ASSIGN(auto insert_result, node.Insert(0, 1, 0, manager));
  EXPECT_THAT(insert_result, (Split<int>{6, PageId(7)}));

  // Check the structure of the new tree. The new siblings have both 2 keys.
  auto& root = CreateNewRoot<Inner>(manager, insert_result);
  EXPECT_EQ(ToTree<2>(root, manager),
            Node(Node(Node(0), 1, Node(2), 3, Node(4, 5)), 6,
                 Node(Node(7, 8), 9, Node(10, 11), 12, Node(13, 14))));
}

TEST(InnerNode, SplitInRightHalfKeepsSiblingsBalanced) {
  using Leaf = LeafNode<int, Unit, std::less<int>, 2>;
  using Inner = InnerNode<Leaf, 4>;
  TestPageManager manager;

  // Create a full tree.
  Inner& node =
      Create<Inner>(manager, Node(Node(1, 2), 3, Node(4, 5), 6, Node(7, 8), 9,
                                  Node(10, 11), 12, Node(13, 14)));

  // Inserting element 15 ends leads to equal balanced inner nodes.
  ASSERT_OK_AND_ASSIGN(auto insert_result, node.Insert(0, 1, 15, manager));
  EXPECT_THAT(insert_result, (Split<int>{9, PageId(7)}));

  // Check the structure of the new tree. The new siblings have both 2 keys.
  auto& root = CreateNewRoot<Inner>(manager, insert_result);
  EXPECT_EQ(ToTree<2>(root, manager),
            Node(Node(Node(1, 2), 3, Node(4, 5), 6, Node(7, 8)), 9,
                 Node(Node(10, 11), 12, Node(13), 14, Node(15))));
}

TEST(InnerNode, InsertingNewEntryInSubTreeDoesNotMarkInnerNodeDirty) {
  using Leaf = LeafNode<int, Unit, std::less<int>, 2>;
  using Inner = InnerNode<Leaf, 2>;
  MockPageManager manager;
  auto& node = Create<Inner>(manager, Node(Node(1), 4, Node(5)));

  EXPECT_CALL(manager, MarkAsDirty(1));  // the {1} node updated to {1,3}
  EXPECT_THAT(node.Insert(12, 1, 3, manager), EntryAdded{});
}

TEST(InnerNode, InsertingNewKeyMarksNodeDirty) {
  using Leaf = LeafNode<int, Unit, std::less<int>, 2>;
  using Inner = InnerNode<Leaf, 2>;
  MockPageManager manager;
  auto& node = Create<Inner>(manager, Node(Node(1, 3), 4, Node(5)));

  EXPECT_CALL(manager, MarkAsDirty(12));  // the inner node targeted
  EXPECT_CALL(manager, MarkAsDirty(1));   // the {1,3} node
  EXPECT_CALL(manager, MarkAsDirty(3));   // the new overflow node
  EXPECT_THAT(node.Insert(12, 1, 2, manager), EntryAdded{});
}

TEST(InnerNode, SplittingMarksTheOldAndNewNodeDirty) {
  using Leaf = LeafNode<int, Unit, std::less<int>, 2>;
  using Inner = InnerNode<Leaf, 2>;
  MockPageManager manager;
  auto& node = Create<Inner>(manager, Node(Node(1, 3), 4, Node(5), 6, Node(7)));

  EXPECT_CALL(manager, MarkAsDirty(12));  // the inner node targeted
  EXPECT_CALL(manager, MarkAsDirty(1));   // the {1,3} node
  EXPECT_CALL(manager, MarkAsDirty(4));   // the new leaf overflow node
  EXPECT_CALL(manager, MarkAsDirty(5));   // the new inner overflow node
  EXPECT_THAT(node.Insert(12, 1, 2, manager), (Split<int>{4, 5}));
}

TEST(InnerNode, ContainsFindsElementsInSubTree) {
  using Leaf = LeafNode<int, Unit, std::less<int>, 4>;
  using Inner = InnerNode<Leaf, 4>;
  TestPageManager manager;

  // Create a full tree.
  Inner& node =
      Create<Inner>(manager, Node(Node(1, 2, 3), 4, Node(5, 6, 7), 8,
                                  Node(9, 10, 11), 12, Node(13, 14, 15)));
  ASSERT_OK(node.Check(1, nullptr, nullptr, manager));

  // Check that those elements that are contained can be found, others not.
  for (int i = 0; i < 20; i++) {
    EXPECT_THAT(node.Contains(1, i, manager), 1 <= i && i <= 15);
  }
}

TEST(InnerNode, CheckRequiresAtLeastOneKeyInRoot) {
  using Leaf = LeafNode<int, Unit, std::less<int>, 2>;
  using Inner = InnerNode<Leaf, 4>;
  TestPageManager manager;
  auto& root = manager.Create<Inner>();

  EXPECT_THAT(
      root.Check(1, nullptr, nullptr, manager),
      StatusIs(kInternal, StrEq("Root node must have at least one key")));
}

TEST(InnerNode, SingleKeyRootNodePassesTest) {
  using Leaf = LeafNode<int, Unit, std::less<int>, 2>;
  using Inner = InnerNode<Leaf, 4>;
  TestPageManager manager;
  auto& root = Create<Inner>(manager, Node(Node(1, 2), 3, Node(4, 5)));

  EXPECT_OK(root.Check(1, nullptr, nullptr, manager));
}

TEST(InnerNode, SingleKeyInnerNodeFailsWithTooFewKeys) {
  using Leaf = LeafNode<int, Unit, std::less<int>, 2>;
  using Inner = InnerNode<Leaf, 4>;
  TestPageManager manager;
  auto& node = Create<Inner>(manager, Node(Node(1, 2), 3, Node(4, 5)));

  int low = 0;
  EXPECT_THAT(
      node.Check(1, &low, nullptr, manager),
      StatusIs(kInternal,
               StrEq("Invalid number of keys, expected at least 2, got 1")));
}

TEST(InnerNode, LargeEnoughInnerNodePasses) {
  using Leaf = LeafNode<int, Unit, std::less<int>, 2>;
  using Inner = InnerNode<Leaf, 4>;
  TestPageManager manager;
  auto& node =
      Create<Inner>(manager, Node(Node(1, 2), 3, Node(4, 5), 6, Node(7, 8)));

  int low = 0;
  EXPECT_OK(node.Check(1, &low, nullptr, manager));
}

TEST(InnerNode, IncorrectlyOrderedKeysAreDetected) {
  using Leaf = LeafNode<int, Unit, std::less<int>, 2>;
  using Inner = InnerNode<Leaf, 4>;
  TestPageManager manager;
  // Keys are not in order, 6 > 3
  auto& node =
      Create<Inner>(manager, Node(Node(1, 2), 6, Node(4, 5), 3, Node(7, 8)));

  EXPECT_THAT(node.Check(1, nullptr, nullptr, manager),
              StatusIs(kInternal, StrEq("Invalid order of keys")));
}

TEST(InnerNode, BoundViolationsAreDetected) {
  using Leaf = LeafNode<int, Unit, std::less<int>, 2>;
  using Inner = InnerNode<Leaf, 4>;
  TestPageManager manager;
  auto& node =
      Create<Inner>(manager, Node(Node(1, 2), 3, Node(4, 5), 6, Node(7, 8)));

  EXPECT_OK(node.Check(1, nullptr, nullptr, manager));

  int limit = 0;
  EXPECT_OK(node.Check(1, &limit, nullptr, manager));
  limit = 3;
  EXPECT_THAT(
      node.Check(1, &limit, nullptr, manager),
      StatusIs(_, StrEq("Lower boundary is not less than smallest key")));
  limit = 6;
  EXPECT_THAT(
      node.Check(1, nullptr, &limit, manager),
      StatusIs(_, StrEq("Biggest key is not less than upper boundary")));
  limit = 9;
  EXPECT_OK(node.Check(1, nullptr, &limit, manager));
}

TEST(InnerNode, ChildErrorsArePropagated) {
  using Leaf = LeafNode<int, Unit, std::less<int>, 4>;
  using Inner = InnerNode<Leaf, 4>;
  TestPageManager manager;
  {
    // The leaf storing 4 has too few entries.
    auto& node =
        Create<Inner>(manager, Node(Node(1, 2), 3, Node(4), 6, Node(7, 8)));
    EXPECT_THAT(
        node.Check(1, nullptr, nullptr, manager),
        StatusIs(
            _, StrEq("Invalid number of entries, expected at least 2, got 1")));
  }
  {
    // The middle leaf is out-of-order.
    auto& node =
        Create<Inner>(manager, Node(Node(1, 2), 3, Node(5, 4), 6, Node(7, 8)));
    EXPECT_THAT(node.Check(1, nullptr, nullptr, manager),
                StatusIs(_, StrEq("Invalid order of entries")));
  }
}

TEST(InnerNode, BoundariesArePropagated) {
  using Leaf = LeafNode<int, Unit, std::less<int>, 4>;
  using Inner = InnerNode<Leaf, 4>;
  TestPageManager manager;
  {
    // The value 2 should not be in the middle node.
    auto& node =
        Create<Inner>(manager, Node(Node(1, 2), 3, Node(2, 5), 6, Node(7, 8)));
    EXPECT_THAT(
        node.Check(1, nullptr, nullptr, manager),
        StatusIs(_, StrEq("Lower boundary is not less than smallest entry")));
  }
  {
    // The value 7 should not be in the middle node.
    auto& node =
        Create<Inner>(manager, Node(Node(1, 2), 3, Node(4, 7), 6, Node(7, 8)));
    EXPECT_THAT(
        node.Check(1, nullptr, nullptr, manager),
        StatusIs(_, StrEq("Biggest entry is not less than upper boundary")));
  }
  {
    auto& node =
        Create<Inner>(manager, Node(Node(1, 2), 3, Node(4, 5), 6, Node(7, 8)));
    int limit = 2;
    EXPECT_THAT(
        node.Check(1, &limit, nullptr, manager),
        StatusIs(_, StrEq("Lower boundary is not less than smallest entry")));
  }
  {
    auto& node =
        Create<Inner>(manager, Node(Node(1, 2), 3, Node(4, 7), 6, Node(7, 8)));
    int limit = 8;
    EXPECT_THAT(
        node.Check(1, nullptr, &limit, manager),
        StatusIs(_, StrEq("Biggest entry is not less than upper boundary")));
  }
}

}  // namespace
}  // namespace carmen::backend::btree
