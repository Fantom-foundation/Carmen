#include "archive/leveldb/archive.h"

#include <type_traits>

#include "archive/archive.h"
#include "common/file_util.h"
#include "common/hash.h"
#include "common/status_test_util.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::archive::leveldb {
namespace {

using ::testing::_;
using ::testing::ElementsAre;
using ::testing::HasSubstr;
using ::testing::IsOkAndHolds;
using ::testing::StatusIs;

TEST(LevelDbArchive, TypeProperties) {
  EXPECT_FALSE(std::is_default_constructible_v<LevelDbArchive>);
  EXPECT_FALSE(std::is_copy_constructible_v<LevelDbArchive>);
  EXPECT_TRUE(std::is_move_constructible_v<LevelDbArchive>);
  EXPECT_FALSE(std::is_copy_assignable_v<LevelDbArchive>);
  EXPECT_TRUE(std::is_move_assignable_v<LevelDbArchive>);
  EXPECT_TRUE(std::is_destructible_v<LevelDbArchive>);

  EXPECT_TRUE(Archive<LevelDbArchive>);
}

TEST(LevelDbArchive, OpenAndClosingEmptyDbWorks) {
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto archive, LevelDbArchive::Open(dir));
  EXPECT_OK(archive.Close());
}

TEST(LevelDbArchive, InAnEmptyArchiveEverythingIsZero) {
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto archive, LevelDbArchive::Open(dir));

  for (BlockId block = 0; block < 5; block++) {
    for (Address addr; addr[0] < 5; addr[0]++) {
      EXPECT_THAT(archive.GetBalance(block, addr), Balance{});
      EXPECT_THAT(archive.GetCode(block, addr), Code{});
      EXPECT_THAT(archive.GetNonce(block, addr), Nonce{});
      /*
      for (Key key; key[0] < 5; key[0]++) {
        EXPECT_THAT(archive.GetStorage(block, addr, key), Value{});
      }
      */
    }
  }
}

TEST(LevelDbArchive, MultipleBalancesOfTheSameAccountCanBeRetained) {
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto archive, LevelDbArchive::Open(dir));

  Address addr{};

  Balance zero{};
  Balance one{0x01};
  Balance two{0x02};

  Update update1;
  update1.Set(addr, one);
  EXPECT_OK(archive.Add(BlockId(2), update1));

  Update update2;
  update2.Set(addr, two);
  EXPECT_OK(archive.Add(BlockId(4), update2));

  EXPECT_THAT(archive.GetBalance(0, addr), zero);
  EXPECT_THAT(archive.GetBalance(1, addr), zero);
  EXPECT_THAT(archive.GetBalance(2, addr), one);
  EXPECT_THAT(archive.GetBalance(3, addr), one);
  EXPECT_THAT(archive.GetBalance(4, addr), two);
  EXPECT_THAT(archive.GetBalance(5, addr), two);
}

TEST(LevelDbArchive, MultipleCodesOfTheSameAccountCanBeRetained) {
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto archive, LevelDbArchive::Open(dir));

  Address addr{};

  Code zero{};
  Code one{0x01};
  Code two{0x02, 0x03};

  Update update1;
  update1.Set(addr, one);
  EXPECT_OK(archive.Add(BlockId(2), update1));

  Update update2;
  update2.Set(addr, two);
  EXPECT_OK(archive.Add(BlockId(4), update2));

  EXPECT_THAT(archive.GetCode(0, addr), zero);
  EXPECT_THAT(archive.GetCode(1, addr), zero);
  EXPECT_THAT(archive.GetCode(2, addr), one);
  EXPECT_THAT(archive.GetCode(3, addr), one);
  EXPECT_THAT(archive.GetCode(4, addr), two);
  EXPECT_THAT(archive.GetCode(5, addr), two);
}

TEST(LevelDbArchive, MultipleNoncesOfTheSameAccountCanBeRetained) {
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto archive, LevelDbArchive::Open(dir));

  Address addr{};

  Nonce zero{};
  Nonce one{0x01};
  Nonce two{0x02};

  Update update1;
  update1.Set(addr, one);
  EXPECT_OK(archive.Add(BlockId(2), update1));

  Update update2;
  update2.Set(addr, two);
  EXPECT_OK(archive.Add(BlockId(4), update2));

  EXPECT_THAT(archive.GetNonce(0, addr), zero);
  EXPECT_THAT(archive.GetNonce(1, addr), zero);
  EXPECT_THAT(archive.GetNonce(2, addr), one);
  EXPECT_THAT(archive.GetNonce(3, addr), one);
  EXPECT_THAT(archive.GetNonce(4, addr), two);
  EXPECT_THAT(archive.GetNonce(5, addr), two);
}

}  // namespace
}  // namespace carmen::archive::leveldb
