#include "backend/index/leveldb/index.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::backend::index {
namespace {

using ::testing::StrEq;

TEST(LevelDBIndexTest, ConvertToLevelDBKey) {
  auto key = internal::ToLevelDBKey(internal::KeySpace::kValue, 1);
  std::string val(1, static_cast<char>(internal::KeySpace::kValue));
  EXPECT_THAT(key, StrEq(val + "1"));
}

TEST(LevelDBIndexTest, ConvertAndParseLevelDBValue) {
  std::uint8_t input = 69;
  auto value = internal::ToLevelDBValue(input);
  EXPECT_EQ(input, internal::ParseLevelDBResult<std::uint8_t>(value));
}

TEST(LevelDBIndexTest, TypeProperties) {
  auto db = LevelDBIndex("/tmp/carmen_test");
  auto balance_index = db.Balance<int,int>();

  auto index = balance_index.GetOrAdd(2);

  EXPECT_EQ(index.value(), 0);
}

}  // namespace
}  // namespace carmen::backend::index
