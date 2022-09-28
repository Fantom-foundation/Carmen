#include "state/state.h"

#include "backend/index/memory/index.h"
#include "backend/store/memory/store.h"
#include "gtest/gtest.h"

namespace carmen {

template <typename K, typename V>
using InMemoryIndex = backend::index::InMemoryIndex<K, V>;

template <typename K, typename V>
using InMemoryStore = backend::store::InMemoryStore<K, V>;

using InMemoryState = State<InMemoryIndex, InMemoryStore>;

TEST(StateTest, DefaultBalanceIsZero) {
  Address a{0x01};
  Address b{0x02};

  InMemoryState state;
  EXPECT_EQ(0, state.GetBalance(a));
  EXPECT_EQ(0, state.GetBalance(b));
}

TEST(StateTest, BalancesCanBeUpdated) {
  Address a{0x01};
  Address b{0x02};

  InMemoryState state;
  EXPECT_EQ(0, state.GetBalance(a));
  EXPECT_EQ(0, state.GetBalance(b));

  state.SetBalance(a, 12);
  EXPECT_EQ(12, state.GetBalance(a));
  EXPECT_EQ(0, state.GetBalance(b));

  state.SetBalance(b, 14);
  EXPECT_EQ(12, state.GetBalance(a));
  EXPECT_EQ(14, state.GetBalance(b));
}

TEST(StateTest, BalancesAreCoveredByGlobalStateHash) {
  InMemoryState state;
  auto base_hash = state.GetHash();
  state.SetBalance({}, 12);
  auto value_12_hash = state.GetHash();
  EXPECT_NE(base_hash, value_12_hash);
  state.SetBalance({}, 14);
  auto value_14_hash = state.GetHash();
  EXPECT_NE(base_hash, value_14_hash);

  // Resetting value gets us original hash.
  state.SetBalance({}, 12);
  auto value_12_hash_again = state.GetHash();
  EXPECT_EQ(value_12_hash, value_12_hash_again);
}

TEST(StateTest, DefaultNonceIsZero) {
  Address a{0x01};
  Address b{0x02};

  InMemoryState state;
  EXPECT_EQ(0, state.GetNonce(a));
  EXPECT_EQ(0, state.GetNonce(b));
}

TEST(StateTest, NoncesCanBeUpdated) {
  Address a{0x01};
  Address b{0x02};

  InMemoryState state;
  EXPECT_EQ(0, state.GetNonce(a));
  EXPECT_EQ(0, state.GetNonce(b));

  state.SetNonce(a, 12);
  EXPECT_EQ(12, state.GetNonce(a));
  EXPECT_EQ(0, state.GetNonce(b));

  state.SetNonce(b, 14);
  EXPECT_EQ(12, state.GetNonce(a));
  EXPECT_EQ(14, state.GetNonce(b));
}

TEST(StateTest, NoncesAreCoveredByGlobalStateHash) {
  InMemoryState state;
  auto base_hash = state.GetHash();
  state.SetNonce({}, 12);
  auto value_12_hash = state.GetHash();
  EXPECT_NE(base_hash, value_12_hash);
  state.SetNonce({}, 14);
  auto value_14_hash = state.GetHash();
  EXPECT_NE(base_hash, value_14_hash);

  // Resetting value gets us original hash.
  state.SetNonce({}, 12);
  auto value_12_hash_again = state.GetHash();
  EXPECT_EQ(value_12_hash, value_12_hash_again);
}

TEST(StateTest, ValuesAddedCanBeRetrieved) {
  Address a;
  Key k;
  Value v{0x01, 0x02};

  InMemoryState state;
  state.SetStorageValue(a, k, v);
  EXPECT_EQ(v, state.GetStorageValue(a, k));

  v = Value{0x03};
  state.SetStorageValue(a, k, v);
  EXPECT_EQ(v, state.GetStorageValue(a, k));
}

}  // namespace carmen
