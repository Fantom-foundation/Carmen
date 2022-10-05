#include "backend/store/memory/store.h"

#include "backend/store/file/file.h"
#include "backend/store/file/store.h"
#include "common/test_util.h"
#include "common/type.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::backend::store {
namespace {

using ::testing::StrEq;

using ReferenceStore = InMemoryStore<int, Value, 32>;

// A test suite testing generic store implementations.
template <typename Store>
class StoreTest : public testing::Test {};

TYPED_TEST_SUITE_P(StoreTest);

TYPED_TEST_P(StoreTest, UninitializedValuesAreZero) {
  TypeParam store;
  EXPECT_EQ(Value{}, store.Get(0));
  EXPECT_EQ(Value{}, store.Get(10));
  EXPECT_EQ(Value{}, store.Get(100));
}

TYPED_TEST_P(StoreTest, DataCanBeAddedAndRetrieved) {
  TypeParam store;
  EXPECT_EQ(Value{}, store.Get(10));
  EXPECT_EQ(Value{}, store.Get(12));

  store.Set(10, Value{12});
  EXPECT_EQ(Value{12}, store.Get(10));
  EXPECT_EQ(Value{}, store.Get(12));

  store.Set(12, Value{14});
  EXPECT_EQ(Value{12}, store.Get(10));
  EXPECT_EQ(Value{14}, store.Get(12));
}

TYPED_TEST_P(StoreTest, EntriesCanBeUpdated) {
  TypeParam store;
  EXPECT_EQ(Value{}, store.Get(10));
  store.Set(10, Value{12});
  EXPECT_EQ(Value{12}, store.Get(10));
  store.Set(10, Value{14});
  EXPECT_EQ(Value{14}, store.Get(10));
}

TYPED_TEST_P(StoreTest, EmptyStoreHasZeroHash) {
  TypeParam store;
  EXPECT_EQ(Hash{}, store.GetHash());
}

TYPED_TEST_P(StoreTest, KnownHashesAreReproduced) {
  TypeParam store;
  EXPECT_EQ(Hash{}, store.GetHash());
  store.Set(0, Value{});
  EXPECT_THAT(Print(store.GetHash()),
              StrEq("0x66687aadf862bd776c8fc18b8e9f8e20089714856ee233b3902a591d"
                    "0d5f2925"));
  store.Set(0, Value{0xAA});
  EXPECT_THAT(Print(store.GetHash()),
              StrEq("0xe7ac50af91de0eca8d6805f0cf111ac4f0937e3136292cace6a50392"
                    "fe905615"));
  store.Set(1, Value{0xBB});
  EXPECT_THAT(Print(store.GetHash()),
              StrEq("0x1e7272c135640b8d6f1bb58f4887f022eddc7f21d077439c14bfb22f"
                    "15952d5d"));
  store.Set(2, Value{0xCC});
  EXPECT_THAT(Print(store.GetHash()),
              StrEq("0xaf87d5bc44995a6d537df52a75ef073ff24581aef087e37ec981035b"
                    "6b0072e4"));
}

TYPED_TEST_P(StoreTest, HashesEqualReferenceImplementation) {
  constexpr int N = 100;
  ReferenceStore reference;
  TypeParam store;
  EXPECT_EQ(Hash{}, store.GetHash());

  for (int i = 0; i < N; i++) {
    Value value{static_cast<unsigned char>(i >> 6 & 0x3),
                static_cast<unsigned char>(i >> 4 & 0x3),
                static_cast<unsigned char>(i >> 2 & 0x3),
                static_cast<unsigned char>(i >> 0 & 0x3)};
    store.Set(i, value);
    reference.Set(i, value);
    EXPECT_EQ(reference.GetHash(), store.GetHash());
  }
}

REGISTER_TYPED_TEST_SUITE_P(StoreTest, UninitializedValuesAreZero,
                            DataCanBeAddedAndRetrieved, EntriesCanBeUpdated,
                            EmptyStoreHasZeroHash, KnownHashesAreReproduced,
                            HashesEqualReferenceImplementation);

using StoreTypes =
    ::testing::Types<ReferenceStore, InMemoryStore<int, Value, 32>,
                     FileStore<int, Value, InMemoryFile, 32> >;

INSTANTIATE_TYPED_TEST_SUITE_P(All, StoreTest, StoreTypes);

}  // namespace
}  // namespace carmen::backend::store
