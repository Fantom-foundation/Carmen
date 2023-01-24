#include "state/archive.h"

#include <type_traits>

#include "common/file_util.h"
#include "common/status_test_util.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen {
namespace {

using ::testing::_;
using ::testing::HasSubstr;
using ::testing::StatusIs;

TEST(Archive, TypeProperties) {
  EXPECT_FALSE(std::is_default_constructible_v<Archive>);
  EXPECT_FALSE(std::is_copy_constructible_v<Archive>);
  EXPECT_TRUE(std::is_move_constructible_v<Archive>);
  EXPECT_FALSE(std::is_copy_assignable_v<Archive>);
  EXPECT_TRUE(std::is_move_assignable_v<Archive>);
  EXPECT_TRUE(std::is_destructible_v<Archive>);
}

TEST(Archive, OpenAndClosingEmptyDbWorks) {
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto archive, Archive::Open(dir));
  EXPECT_OK(archive.Close());
}

TEST(Archive, InAnEmptyArchiveStorageIsZero) {
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto archive, Archive::Open(dir));

  const Value zero{};
  for (BlockId block = 0; block < 5; block++) {
    for (Address addr; addr[0] < 5; addr[0]++) {
      for (Key key; key[0] < 5; key[0]++) {
        EXPECT_THAT(archive.GetStorage(block, addr, key), zero);
      }
    }
  }
}

TEST(Archive, MultipleVersionsOfTheSameValueCanBeRetained) {
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto archive, Archive::Open(dir));

  Address addr;
  Key key;

  Value zero;
  Value one{0x01};
  Value two{0x02};

  BlockUpdate update1;
  update1.Set(addr, key, one);
  EXPECT_OK(archive.Add(BlockId(2), update1));

  BlockUpdate update2;
  update2.Set(addr, key, two);
  EXPECT_OK(archive.Add(BlockId(4), update2));

  EXPECT_THAT(archive.GetStorage(0, addr, key), zero);
  EXPECT_THAT(archive.GetStorage(1, addr, key), zero);
  EXPECT_THAT(archive.GetStorage(2, addr, key), one);
  EXPECT_THAT(archive.GetStorage(3, addr, key), one);
  EXPECT_THAT(archive.GetStorage(4, addr, key), two);
  EXPECT_THAT(archive.GetStorage(5, addr, key), two);
}

TEST(Archive, DifferentAccountsAreDifferentiated) {
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto archive, Archive::Open(dir));

  Address addr1{0x01};
  Address addr2{0x02};
  Key key1{0x01};
  Key key2{0x02};

  Value zero;
  Value one{0x01};
  Value two{0x02};

  BlockUpdate update1;
  update1.Set(addr1, key1, one);
  update1.Set(addr1, key2, two);
  update1.Set(addr2, key1, two);
  update1.Set(addr2, key2, one);
  EXPECT_OK(archive.Add(BlockId(1), update1));

  EXPECT_THAT(archive.GetStorage(0, addr1, key1), zero);
  EXPECT_THAT(archive.GetStorage(1, addr1, key1), one);
  EXPECT_THAT(archive.GetStorage(2, addr1, key1), one);

  EXPECT_THAT(archive.GetStorage(0, addr1, key2), zero);
  EXPECT_THAT(archive.GetStorage(1, addr1, key2), two);
  EXPECT_THAT(archive.GetStorage(2, addr1, key2), two);

  EXPECT_THAT(archive.GetStorage(0, addr2, key1), zero);
  EXPECT_THAT(archive.GetStorage(1, addr2, key1), two);
  EXPECT_THAT(archive.GetStorage(2, addr2, key1), two);

  EXPECT_THAT(archive.GetStorage(0, addr2, key2), zero);
  EXPECT_THAT(archive.GetStorage(1, addr2, key2), one);
  EXPECT_THAT(archive.GetStorage(2, addr2, key2), one);
}

TEST(Archive, ConflictingHistoryCanNotBeAdded) {
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto archive, Archive::Open(dir));

  BlockId block = 2;
  Address addr;
  Key key;

  Value one{0x01};
  Value two{0x02};

  BlockUpdate update1;
  update1.Set(addr, key, one);
  EXPECT_OK(archive.Add(block, update1));
  EXPECT_THAT(archive.GetStorage(block, addr, key), one);

  // Attempting to update the same block again fails.
  BlockUpdate update2;
  update2.Set(addr, key, two);
  EXPECT_THAT(archive.Add(block, update2),
              StatusIs(_, HasSubstr("UNIQUE constraint failed")));

  // The storage remains as it was.
  EXPECT_THAT(archive.GetStorage(block, addr, key), one);
}

}  // namespace
}  // namespace carmen
