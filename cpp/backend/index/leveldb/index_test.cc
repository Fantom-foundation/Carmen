#include "backend/index/leveldb/index.h"

#include "absl/status/statusor.h"
#include "backend/index/test_util.h"
#include "common/file_util.h"
#include "common/type.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::backend::index {
namespace {

using ::testing::StrEq;

// LevelDB index type definition for the generic index tests.
template <Trivial K, std::integral I>
class LevelDBIndexTestType {
 public:
  using key_type [[maybe_unused]] = K;
  using value_type [[maybe_unused]] = I;
  LevelDBIndexTestType()
      : dir_{},
        adapter_(
            LevelDBIndex(dir_.GetPath().string()).KeySpace<int, int>('t')) {}
  LevelDBIndexTestType(LevelDBIndexTestType&&) noexcept {
      // fake move constructor to make test suite pass (TempDir is not movable)
      // instead test the real move constructor in TypeProperties test
  };
  decltype(auto) GetOrAdd(auto key) { return adapter_.GetOrAdd(key); }
  decltype(auto) Get(auto key) { return adapter_.Get(key); }
  decltype(auto) GetHash() { return adapter_.GetHash(); }

 private:
  TempDir dir_;
  LevelDBKeySpaceAdapter<K, I> adapter_;
};

using TestIndex = LevelDBIndexTestType<int, int>;

// Instantiates common index tests for the Cached index type.
INSTANTIATE_TYPED_TEST_SUITE_P(LevelDB, IndexTest, TestIndex);

LevelDBKeySpace<int, int> GetTestIndex(const TempDir& dir) {
  return LevelDBIndex(dir.GetPath().string()).KeySpace<int, int>('t');
}

TEST(LevelDBIndexTest, TypeProperties) {
  using LevelDBKeySpace = LevelDBKeySpace<int, int>;
  EXPECT_TRUE(std::is_move_constructible_v<LevelDBKeySpace>);
}

TEST(LevelDBIndexTest, ConvertToLevelDBKey) {
  int key = 21;
  auto res = internal::ToDBKey('A', key);
  std::stringstream ss;
  ss << 'A';
  ss.write(reinterpret_cast<const char*>(&key), sizeof(key));
  EXPECT_THAT(res, StrEq(ss.str()));
}

TEST(LevelDBIndexTest, ConvertAndParseLevelDBValue) {
  std::uint8_t input = 69;
  auto value = internal::ToDBValue(input);
  EXPECT_EQ(input, *internal::ParseDBResult<std::uint8_t>(value));
}

TEST(LevelDBIndexTest, IndexIsPersistent) {
  TempDir dir = TempDir();
  absl::StatusOr<std::pair<int, bool>> result;

  // Insert value in a separate block to ensure that the index is closed.
  {
    auto index = GetTestIndex(dir);
    EXPECT_THAT(index.Get(1).status().code(), absl::StatusCode::kNotFound);
    result = index.GetOrAdd(1);
    EXPECT_EQ((*result).second, true);
    EXPECT_THAT(*index.Get(1), (*result).first);
  }

  // Reopen index and check that the value is still present.
  {
    auto index = GetTestIndex(dir);
    EXPECT_THAT(*index.Get(1), (*result).first);
  }
}

}  // namespace
}  // namespace carmen::backend::index
