#include "backend/depot/memory/depot.h"

#include "backend/depot/depot_handler.h"
#include "backend/depot/file/depot.h"
#include "backend/depot/leveldb/depot.h"
#include "common/status_test_util.h"
#include "common/test_util.h"
#include "gmock/gmock-matchers.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::backend::depot {
namespace {

using ::testing::_;
using ::testing::ElementsAre;
using ::testing::StatusIs;
using ::testing::StrEq;

// A test suite testing generic depot implementations.
template <typename>
class DepotTest : public testing::Test {};

TYPED_TEST_SUITE_P(DepotTest);

TYPED_TEST_P(DepotTest, TypeProperties) {
  TypeParam wrapper;
  auto& depot = wrapper.GetDepot();
  EXPECT_TRUE(std::is_move_constructible_v<decltype(depot)>);
}

TYPED_TEST_P(DepotTest, DataCanBeAddedAndRetrieved) {
  TypeParam wrapper;
  auto& depot = wrapper.GetDepot();

  EXPECT_THAT(depot.Get(10), StatusIs(absl::StatusCode::kNotFound, _));
  EXPECT_THAT(depot.Get(100), StatusIs(absl::StatusCode::kNotFound, _));

  EXPECT_OK(depot.Set(10, std::array{std::byte{1}, std::byte{2}}));
  ASSERT_OK_AND_ASSIGN(auto val, depot.Get(10));
  EXPECT_THAT(val, ElementsAre(std::byte{1}, std::byte{2}));

  EXPECT_OK(
      depot.Set(100, std::array{std::byte{1}, std::byte{2}, std::byte{3}}));
  ASSERT_OK_AND_ASSIGN(val, depot.Get(100));
  EXPECT_THAT(val, ElementsAre(std::byte{1}, std::byte{2}, std::byte{3}));
}

TYPED_TEST_P(DepotTest, EntriesCanBeUpdated) {
  TypeParam wrapper;
  auto& depot = wrapper.GetDepot();

  EXPECT_OK(depot.Set(10, std::array{std::byte{1}, std::byte{2}}));
  ASSERT_OK_AND_ASSIGN(auto val, depot.Get(10));
  EXPECT_THAT(val, ElementsAre(std::byte{1}, std::byte{2}));

  EXPECT_OK(
      depot.Set(10, std::array{std::byte{1}, std::byte{2}, std::byte{3}}));
  ASSERT_OK_AND_ASSIGN(val, depot.Get(10));
  EXPECT_THAT(val, ElementsAre(std::byte{1}, std::byte{2}, std::byte{3}));
}

TYPED_TEST_P(DepotTest, EmptyDepotHasZeroHash) {
  TypeParam wrapper;
  auto& depot = wrapper.GetDepot();
  ASSERT_OK_AND_ASSIGN(auto hash, depot.GetHash());
  EXPECT_EQ(Hash{}, hash);
}

TYPED_TEST_P(DepotTest, NonEmptyDepotHasHash) {
  TypeParam wrapper;
  auto& depot = wrapper.GetDepot();

  ASSERT_OK_AND_ASSIGN(auto initial_hash, depot.GetHash());
  EXPECT_OK(depot.Set(10, std::array{std::byte{1}, std::byte{2}}));
  ASSERT_OK_AND_ASSIGN(auto new_hash, depot.GetHash());
  ASSERT_NE(initial_hash, new_hash);
}

TYPED_TEST_P(DepotTest, HashChangesBack) {
  TypeParam wrapper;
  auto& depot = wrapper.GetDepot();

  EXPECT_OK(depot.Set(10, std::array{std::byte{1}, std::byte{2}}));
  EXPECT_OK(
      depot.Set(100, std::array{std::byte{1}, std::byte{2}, std::byte{3}}));
  ASSERT_OK_AND_ASSIGN(auto initial_hash, depot.GetHash());

  EXPECT_OK(
      depot.Set(10, std::array{std::byte{1}, std::byte{2}, std::byte{3}}));
  ASSERT_OK_AND_ASSIGN(auto new_hash, depot.GetHash());

  ASSERT_NE(initial_hash, new_hash);

  EXPECT_OK(depot.Set(10, std::array{std::byte{1}, std::byte{2}}));
  ASSERT_OK_AND_ASSIGN(new_hash, depot.GetHash());

  ASSERT_EQ(initial_hash, new_hash);
}

TYPED_TEST_P(DepotTest, KnownHashesAreReproduced) {
  if (TypeParam::kBranchingFactor != 3 || TypeParam::kHashBoxSize != 2) {
    GTEST_SKIP()
        << "This test is only valid for branching factor 3 and hash box size "
           "of 2.";
  }

  TypeParam wrapper;
  auto& depot = wrapper.GetDepot();

  // Tests the hashes for values [0x00], [0x00, 0x11] ... [..., 0xFF] inserted
  // in sequence.
  std::vector<std::string> hashes{
      "0x6e340b9cffb37a989ca544e6bb780a2c78901d3fb33738768511a30617afa01d",
      "0xaea3b18a4991da51ab201722c233c967e9c5d726cbc9a327c42b17d24268303b",
      "0xe136dc145513327cf5846ea5cbb3b9d30543d27963288dd7bf6ad63360085df8",
      "0xa98672f2a05a5b71b49451e85238e3f4ebc6fb8cedb00d55d8bc4ea6e52d0117",
      "0x1e7f4c505dd16f8537bdad064b49a8c0a64a707725fbf09ad4311f280781e9e4",
      "0xb07ee4eec6d898d88ec3ef9c66c64f3f0896cd1c7e759b825baf541d42e77784",
      "0x9346102f81ac75e583499081d9ab10c7050ff682c7dfd4700a9f909ee469a2de",
      "0x44532b1bcf3840a8bf0ead0a6052d4968c5fac6023cd1f86ad43175e53d25e9c",
      "0x2de0363a6210fca91e2143b945a86f42ae90cd786e641d51e2a7b9c141b020b0",
      "0x0c81b39c90852a66f18b0518d36dceb2f889501dc279e759bb2d1253a63caa8e",
      "0xc9fa5b094c4d964bf6d2b25d7ba1e580a83b9ebf2ea8594e99baa81474be4c47",
      "0x078fb14729015631017d2d82c844642ec723e92e06eb41f88ca83b36e3a04d30",
      "0x4f91e8c410a52b53e46f7b787fdc240c3349711108c2a1ac69ddb0c64e51f918",
      "0x4e0d2c84af4f9e54c2d0864302a72703c656996585ec99f7290a2172617ea0e9",
      "0x38e68d99bafc836105e88a1092ebdadb6d8a4a1acec29eecc7ec01b885e6f820",
      "0xf9764b20bf761bd89b3266697fbc1c336548c3bcbb1c81e4ecf3829df53d98ec",
  };

  int i = 0;
  std::vector<std::byte> data;
  for (const auto& expected_hash : hashes) {
    data.push_back(static_cast<std::byte>(i << 4 | i));
    EXPECT_OK(depot.Set(i, data));
    ASSERT_OK_AND_ASSIGN(auto actual_hash, depot.GetHash());
    EXPECT_THAT(Print(actual_hash), StrEq(expected_hash));
    i++;
  }
}

TYPED_TEST_P(DepotTest, HashesEqualReferenceImplementation) {
  constexpr int N = 100;
  TypeParam wrapper;
  auto& depot = wrapper.GetDepot();
  auto& reference = wrapper.GetReferenceDepot();

  ASSERT_OK_AND_ASSIGN(auto empty_hash, depot.GetHash());
  EXPECT_EQ(Hash{}, empty_hash);

  std::array<std::byte, 4> value{};
  for (int i = 0; i < N; i++) {
    value = {static_cast<std::byte>(i >> 6 & 0x3),
             static_cast<std::byte>(i >> 4 & 0x3),
             static_cast<std::byte>(i >> 2 & 0x3),
             static_cast<std::byte>(i >> 0 & 0x3)};
    ASSERT_OK(depot.Set(i, value));
    ASSERT_OK(reference.Set(i, value));
    ASSERT_OK_AND_ASSIGN(auto hash, depot.GetHash());
    ASSERT_OK_AND_ASSIGN(auto reference_hash, reference.GetHash());
    EXPECT_EQ(hash, reference_hash);
  }
}

REGISTER_TYPED_TEST_SUITE_P(DepotTest, TypeProperties,
                            DataCanBeAddedAndRetrieved, EntriesCanBeUpdated,
                            EmptyDepotHasZeroHash, NonEmptyDepotHasHash,
                            HashChangesBack, KnownHashesAreReproduced,
                            HashesEqualReferenceImplementation);

using DepotTypes = ::testing::Types<
    // Branching size 3, Size of box 1.
    DepotHandler<InMemoryDepot<unsigned int>, 3, 1>,
    DepotHandler<LevelDbDepot<unsigned int>, 3, 1>,
    DepotHandler<FileDepot<unsigned int>, 3, 1>,

    // Branching size 3, Size of box 2.
    DepotHandler<InMemoryDepot<unsigned int>, 3, 2>,
    DepotHandler<LevelDbDepot<unsigned int>, 3, 2>,
    DepotHandler<FileDepot<unsigned int>, 3, 2>,

    // Branching size 16, Size of box 8.
    DepotHandler<InMemoryDepot<unsigned int>, 16, 8>,
    DepotHandler<LevelDbDepot<unsigned int>, 16, 8>,
    DepotHandler<FileDepot<unsigned int>, 16, 8>,

    // Branching size 32, Size of box 16.
    DepotHandler<InMemoryDepot<unsigned int>, 32, 16>,
    DepotHandler<LevelDbDepot<unsigned int>, 32, 16>,
    DepotHandler<FileDepot<unsigned int>, 32, 16>>;

INSTANTIATE_TYPED_TEST_SUITE_P(All, DepotTest, DepotTypes);

}  // namespace
}  // namespace carmen::backend::depot
