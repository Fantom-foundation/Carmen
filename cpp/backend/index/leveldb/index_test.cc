#include "backend/index/leveldb/index.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::backend::index {
namespace {


TEST(LevelDBIndexTest, TypeProperties) {
  auto db = LevelDBIndexImpl<int, int>();

  auto index = db.GetOrAdd(0);

  EXPECT_EQ(0, 0);
}

}  // namespace
}  // namespace carmen::backend::index
