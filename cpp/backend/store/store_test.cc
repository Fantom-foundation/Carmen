
#include "backend/store/memory/store.h"

#include <string>
#include <vector>

#include "backend/common/file.h"
#include "backend/store/file/store.h"
#include "backend/store/store_handler.h"
#include "common/hash.h"
#include "common/test_util.h"
#include "common/type.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::backend::store {
namespace {

using ::testing::StrEq;

// A test suite testing generic store implementations.
template <typename StoreHandler>
class StoreTest : public testing::Test {};

TYPED_TEST_SUITE_P(StoreTest);

TYPED_TEST_P(StoreTest, UninitializedValuesAreZero) {
  TypeParam wrapper;
  auto& store = wrapper.GetStore();
  EXPECT_EQ(Value{}, store.Get(0));
  EXPECT_EQ(Value{}, store.Get(10));
  EXPECT_EQ(Value{}, store.Get(100));
}

TYPED_TEST_P(StoreTest, DataCanBeAddedAndRetrieved) {
  TypeParam wrapper;
  auto& store = wrapper.GetStore();
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
  TypeParam wrapper;
  auto& store = wrapper.GetStore();
  EXPECT_EQ(Value{}, store.Get(10));
  store.Set(10, Value{12});
  EXPECT_EQ(Value{12}, store.Get(10));
  store.Set(10, Value{14});
  EXPECT_EQ(Value{14}, store.Get(10));
}

TYPED_TEST_P(StoreTest, EmptyStoreHasZeroHash) {
  TypeParam wrapper;
  auto& store = wrapper.GetStore();
  EXPECT_EQ(Hash{}, store.GetHash());
}

TYPED_TEST_P(StoreTest, KnownHashesAreReproduced) {
  // We only hard-code hashes for a subset of the configurations.
  TypeParam wrapper;
  auto& store = wrapper.GetStore();
  EXPECT_EQ(Hash{}, store.GetHash());

  if (TypeParam::kPageSize == 32 && TypeParam::kBranchingFactor == 32) {
    store.Set(0, Value{});
    EXPECT_THAT(
        Print(store.GetHash()),
        StrEq("0x66687aadf862bd776c8fc18b8e9f8e20089714856ee233b3902a591d"
              "0d5f2925"));
    store.Set(0, Value{0xAA});
    EXPECT_THAT(
        Print(store.GetHash()),
        StrEq("0xe7ac50af91de0eca8d6805f0cf111ac4f0937e3136292cace6a50392"
              "fe905615"));
    store.Set(1, Value{0xBB});
    EXPECT_THAT(
        Print(store.GetHash()),
        StrEq("0x1e7272c135640b8d6f1bb58f4887f022eddc7f21d077439c14bfb22f"
              "15952d5d"));
    store.Set(2, Value{0xCC});
    EXPECT_THAT(
        Print(store.GetHash()),
        StrEq("0xaf87d5bc44995a6d537df52a75ef073ff24581aef087e37ec981035b"
              "6b0072e4"));
  }

  if (TypeParam::kPageSize == 64 && TypeParam::kBranchingFactor == 3) {
    // Tests the hashes for values 0x00, 0x11 ... 0xFF inserted in sequence.
    std::vector<std::string> hashes{
        "0xf5a5fd42d16a20302798ef6ed309979b43003d2320d9f0e8ea9831a92759fb4b",
        "0x967293ee9d7ba679c3ef076bef139e2ceb96d45d19a624cc59bb5a3c1649ce38",
        "0x37617dfcbf34b6bd41ef1ba985de1e68b69bf4e42815981868abde09e9e09f0e",
        "0x735e056698bd4b4953a9838c4526c4d2138efd1aee9a94ff36ca100f16a77581",
        "0xc1e116b85f59f2ef61d6a64e61947e33c383f0adf252a3249b6172286ca244aa",
        "0x6001791dfa74121b9d177091606ebcd352e784ecfab05563c40b7ce8346c6f98",
        "0x57aee44f007524162c86d8ab0b1c67ed481c44d248c5f9c48fca5a5368d3a705",
        "0xdd29afc37e669458a3f4509023bf5a362f0c0cdc9bb206a6955a8f5124d26086",
        "0x0ab5ad3ab4f3efb90994cdfd72b2aa0532cc0f9708ea8fb8555677053583e161",
        "0x901d25766654678c6fe19c3364f34f9ed7b649514b9b5b25389de3bbfa346957",
        "0x50743156d6a4967c165a340166d31ca986ceebbb1812aebb3ce744ce7cffaa99",
        "0x592fd0da56dbc41e7ae8d4572c47fe12492eca9ae68b8786ebc322c2e2d61de2",
        "0xbc57674bfa2b806927af318a51025d833f5950ed6cdab5af3c8a876dac5ba1c4",
        "0x6523527158ccde9ed47932da61fed960019843f31f1fdbab3d18958450a00e0f",
        "0xe1bf187a4cd645c7adae643070f070dcb9c4aa8bbc0aded07b99dda3bac6b0ea",
        "0x9a5be401e5aa0b2b31a3b055811b15041f4842be6cd4cb146f3c2b48e2081e19",
        "0x6f060e465bb1b155a6b4822a13b704d3986ab43d7928c14b178e07a8f7673951",
    };
    int i = 0;
    for (auto hash : hashes) {
      store.Set(i, Value{static_cast<std::uint8_t>(i << 4 | i)});
      EXPECT_THAT(Print(store.GetHash()), StrEq(hash));
      i++;
    }
  }
}

TYPED_TEST_P(StoreTest, HashesRespectBranchingFactor) {
  // This test computes the hash expected for a store containing 2*branching
  // factor empty pages.
  static_assert(TypeParam::kPageSize % sizeof(Value) == 0);
  constexpr auto kElementsPerPage = TypeParam::kPageSize / sizeof(Value);
  TypeParam wrapper;
  auto& store = wrapper.GetStore();

  // Initialize branching_factor * 2 pages.
  store.Set(kElementsPerPage * TypeParam::kBranchingFactor * 2 - 1, Value{});

  // This should result in a hash tree with branching_factor * 2 leaves and one
  // inner node forming the root.
  Sha256Hasher hasher;

  // Get the hash of an empty page.
  std::array<std::byte, TypeParam::kPageSize> page{};
  Hash page_hash = GetHash(hasher, page);

  // Get the combined hash of branching-factor many empty pages.
  hasher.Reset();
  for (std::size_t i = 0; i < TypeParam::kBranchingFactor; i++) {
    hasher.Ingest(page_hash);
  }
  Hash block_hash = hasher.GetHash();

  // Compute the hash of the inner node, consisting of two block hashes followed
  // by zero hashes.
  hasher.Reset();
  hasher.Ingest(block_hash);
  hasher.Ingest(block_hash);
  for (std::size_t i = 2; i < TypeParam::kBranchingFactor; i++) {
    hasher.Ingest(Hash{});
  }
  Hash root_hash = hasher.GetHash();

  EXPECT_EQ(root_hash, store.GetHash());
}

TYPED_TEST_P(StoreTest, HashesEqualReferenceImplementation) {
  constexpr int N = 100;
  TypeParam wrapper;
  auto& store = wrapper.GetStore();
  auto& reference = wrapper.GetReferenceStore();

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
                            HashesRespectBranchingFactor,
                            HashesEqualReferenceImplementation);

using StoreTypes = ::testing::Types<
    // Page size 32, branching size 32.
    StoreHandler<ReferenceStore<32>, 32>,
    StoreHandler<InMemoryStore<int, Value, 32>, 32>,
    StoreHandler<FileStore<int, Value, InMemoryFile, 32>, 32>,
    StoreHandler<FileStore<int, Value, SingleFile, 32>, 32>,

    // Page size 64, branching size 3.
    StoreHandler<ReferenceStore<64>, 3>,
    StoreHandler<InMemoryStore<int, Value, 64>, 3>,
    StoreHandler<FileStore<int, Value, InMemoryFile, 64>, 3>,
    StoreHandler<FileStore<int, Value, SingleFile, 64>, 3>,

    // Page size 64, branching size 8.
    StoreHandler<ReferenceStore<64>, 8>,
    StoreHandler<InMemoryStore<int, Value, 64>, 8>,
    StoreHandler<FileStore<int, Value, InMemoryFile, 64>, 8>,
    StoreHandler<FileStore<int, Value, SingleFile, 64>, 8>,

    // Page size 128, branching size 4.
    StoreHandler<ReferenceStore<128>, 4>,
    StoreHandler<InMemoryStore<int, Value, 128>, 4>,
    StoreHandler<FileStore<int, Value, InMemoryFile, 128>, 4>,
    StoreHandler<FileStore<int, Value, SingleFile, 128>, 4>>;

INSTANTIATE_TYPED_TEST_SUITE_P(All, StoreTest, StoreTypes);

}  // namespace
}  // namespace carmen::backend::store
