#include "state/archive.h"

#include <type_traits>

#include "common/file_util.h"
#include "common/hash.h"
#include "common/status_test_util.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen {
namespace {

using ::testing::_;
using ::testing::ElementsAre;
using ::testing::HasSubstr;
using ::testing::IsOkAndHolds;
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

TEST(Archive, InAnEmptyArchiveEverythingIsZero) {
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto archive, Archive::Open(dir));

  for (BlockId block = 0; block < 5; block++) {
    for (Address addr; addr[0] < 5; addr[0]++) {
      EXPECT_THAT(archive.GetBalance(block, addr), Balance{});
      EXPECT_THAT(archive.GetCode(block, addr), Code{});
      EXPECT_THAT(archive.GetNonce(block, addr), Nonce{});
      for (Key key; key[0] < 5; key[0]++) {
        EXPECT_THAT(archive.GetStorage(block, addr, key), Value{});
      }
    }
  }
}

TEST(Archive, MultipleBalancesOfTheSameAccountCanBeRetained) {
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto archive, Archive::Open(dir));

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

TEST(Archive, MultipleCodesOfTheSameAccountCanBeRetained) {
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto archive, Archive::Open(dir));

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

TEST(Archive, MultipleNoncesOfTheSameAccountCanBeRetained) {
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto archive, Archive::Open(dir));

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

TEST(Archive, MultipleValuesOfTheSameSlotCanBeRetained) {
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto archive, Archive::Open(dir));

  Address addr{};
  Key key{};

  Value zero{};
  Value one{0x01};
  Value two{0x02};

  Update update1;
  update1.Set(addr, key, one);
  EXPECT_OK(archive.Add(BlockId(2), update1));

  Update update2;
  update2.Set(addr, key, two);
  EXPECT_OK(archive.Add(BlockId(4), update2));

  EXPECT_THAT(archive.GetStorage(0, addr, key), zero);
  EXPECT_THAT(archive.GetStorage(1, addr, key), zero);
  EXPECT_THAT(archive.GetStorage(2, addr, key), one);
  EXPECT_THAT(archive.GetStorage(3, addr, key), one);
  EXPECT_THAT(archive.GetStorage(4, addr, key), two);
  EXPECT_THAT(archive.GetStorage(5, addr, key), two);
}

TEST(Archive, BalancesOfDifferentAccountsAreDifferentiated) {
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto archive, Archive::Open(dir));

  Address addr1{0x01};
  Address addr2{0x02};

  Balance zero{};
  Balance one{0x01};
  Balance two{0x02};

  Update update1;
  update1.Set(addr1, one);
  update1.Set(addr2, two);
  EXPECT_OK(archive.Add(BlockId(1), update1));

  EXPECT_THAT(archive.GetBalance(0, addr1), zero);
  EXPECT_THAT(archive.GetBalance(1, addr1), one);
  EXPECT_THAT(archive.GetBalance(2, addr1), one);

  EXPECT_THAT(archive.GetBalance(0, addr2), zero);
  EXPECT_THAT(archive.GetBalance(1, addr2), two);
  EXPECT_THAT(archive.GetBalance(2, addr2), two);
}

TEST(Archive, CodesOfDifferentAccountsAreDifferentiated) {
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto archive, Archive::Open(dir));

  Address addr1{0x01};
  Address addr2{0x02};

  Code zero{};
  Code one{0x01};
  Code two{0x02, 0x03};

  Update update1;
  update1.Set(addr1, one);
  update1.Set(addr2, two);
  EXPECT_OK(archive.Add(BlockId(1), update1));

  EXPECT_THAT(archive.GetCode(0, addr1), zero);
  EXPECT_THAT(archive.GetCode(1, addr1), one);
  EXPECT_THAT(archive.GetCode(2, addr1), one);

  EXPECT_THAT(archive.GetCode(0, addr2), zero);
  EXPECT_THAT(archive.GetCode(1, addr2), two);
  EXPECT_THAT(archive.GetCode(2, addr2), two);
}

TEST(Archive, NoncesOfDifferentAccountsAreDifferentiated) {
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto archive, Archive::Open(dir));

  Address addr1{0x01};
  Address addr2{0x02};

  Nonce zero{};
  Nonce one{0x01};
  Nonce two{0x02, 0x03};

  Update update1;
  update1.Set(addr1, one);
  update1.Set(addr2, two);
  EXPECT_OK(archive.Add(BlockId(1), update1));

  EXPECT_THAT(archive.GetNonce(0, addr1), zero);
  EXPECT_THAT(archive.GetNonce(1, addr1), one);
  EXPECT_THAT(archive.GetNonce(2, addr1), one);

  EXPECT_THAT(archive.GetNonce(0, addr2), zero);
  EXPECT_THAT(archive.GetNonce(1, addr2), two);
  EXPECT_THAT(archive.GetNonce(2, addr2), two);
}

TEST(Archive, ValuesOfDifferentAccountsAreDifferentiated) {
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto archive, Archive::Open(dir));

  Address addr1{0x01};
  Address addr2{0x02};
  Key key1{0x01};
  Key key2{0x02};

  Value zero{};
  Value one{0x01};
  Value two{0x02};

  Update update1;
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

TEST(Archive, CreatingAnAccountUpdatesItsExistenceState) {
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto archive, Archive::Open(dir));

  Address addr{0x01};

  Update update;
  update.Create(addr);
  EXPECT_OK(archive.Add(1, update));

  EXPECT_THAT(archive.Exists(0, addr), IsOkAndHolds(false));
  EXPECT_THAT(archive.Exists(1, addr), IsOkAndHolds(true));
  EXPECT_THAT(archive.Exists(2, addr), IsOkAndHolds(true));
}

TEST(Archive, DeletingAnNonExistingAccountKeepsAccountNonExisting) {
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto archive, Archive::Open(dir));

  Address addr{0x01};

  Update update;
  update.Delete(addr);
  EXPECT_OK(archive.Add(1, update));

  EXPECT_THAT(archive.Exists(0, addr), IsOkAndHolds(false));
  EXPECT_THAT(archive.Exists(1, addr), IsOkAndHolds(false));
  EXPECT_THAT(archive.Exists(2, addr), IsOkAndHolds(false));
}

TEST(Archive, DeletingAnExistingAccountKeepsMakesAccountNonExisting) {
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto archive, Archive::Open(dir));

  Address addr{0x01};

  Update update1;
  update1.Create(addr);
  EXPECT_OK(archive.Add(1, update1));

  Update update2;
  update2.Delete(addr);
  EXPECT_OK(archive.Add(3, update2));

  EXPECT_THAT(archive.Exists(0, addr), IsOkAndHolds(false));
  EXPECT_THAT(archive.Exists(1, addr), IsOkAndHolds(true));
  EXPECT_THAT(archive.Exists(2, addr), IsOkAndHolds(true));
  EXPECT_THAT(archive.Exists(3, addr), IsOkAndHolds(false));
  EXPECT_THAT(archive.Exists(4, addr), IsOkAndHolds(false));
}

TEST(Archive, AccountCanBeRecreatedWithoutDelete) {
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto archive, Archive::Open(dir));

  Address addr{0x01};

  Update update1;
  update1.Create(addr);
  EXPECT_OK(archive.Add(1, update1));

  Update update2;
  update2.Create(addr);
  EXPECT_OK(archive.Add(3, update2));

  EXPECT_THAT(archive.Exists(0, addr), IsOkAndHolds(false));
  EXPECT_THAT(archive.Exists(1, addr), IsOkAndHolds(true));
  EXPECT_THAT(archive.Exists(2, addr), IsOkAndHolds(true));
  EXPECT_THAT(archive.Exists(3, addr), IsOkAndHolds(true));
  EXPECT_THAT(archive.Exists(4, addr), IsOkAndHolds(true));
}

TEST(Archive, DeletingAnAccountInvalidatesStorage) {
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto archive, Archive::Open(dir));

  Address addr{0x01};
  Key key{0x02};
  Value zero{0x00};
  Value one{0x01};

  Update update1;
  update1.Create(addr);
  update1.Set(addr, key, one);
  EXPECT_OK(archive.Add(1, update1));

  Update update2;
  update2.Delete(addr);
  EXPECT_OK(archive.Add(3, update2));

  EXPECT_THAT(archive.GetStorage(0, addr, key), zero);
  EXPECT_THAT(archive.GetStorage(1, addr, key), one);
  EXPECT_THAT(archive.GetStorage(2, addr, key), one);
  EXPECT_THAT(archive.GetStorage(3, addr, key), zero);
  EXPECT_THAT(archive.GetStorage(4, addr, key), zero);
}

TEST(Archive, RecreatingAnAccountInvalidatesStorage) {
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto archive, Archive::Open(dir));

  Address addr{0x01};
  Key key{0x02};
  Value zero{0x00};
  Value one{0x01};

  Update update1;
  update1.Create(addr);
  update1.Set(addr, key, one);
  EXPECT_OK(archive.Add(1, update1));

  Update update2;
  update2.Create(addr);
  EXPECT_OK(archive.Add(3, update2));

  EXPECT_THAT(archive.GetStorage(0, addr, key), zero);
  EXPECT_THAT(archive.GetStorage(1, addr, key), one);
  EXPECT_THAT(archive.GetStorage(2, addr, key), one);
  EXPECT_THAT(archive.GetStorage(3, addr, key), zero);
  EXPECT_THAT(archive.GetStorage(4, addr, key), zero);
}

TEST(Archive, StorageOfRecreatedAccountCanBeUpdated) {
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto archive, Archive::Open(dir));

  Address addr{0x01};

  Key key1{0x01};  // used in old and new account
  Key key2{0x02};  // used only in old account
  Key key3{0x03};  // used only in new account

  Value zero{0x00};
  Value one{0x01};
  Value two{0x02};

  Update update1;
  update1.Create(addr);
  update1.Set(addr, key1, one);
  update1.Set(addr, key2, two);
  EXPECT_OK(archive.Add(1, update1));

  Update update2;
  update2.Create(addr);
  update2.Set(addr, key1, two);
  update2.Set(addr, key3, one);
  EXPECT_OK(archive.Add(3, update2));

  EXPECT_THAT(archive.GetStorage(0, addr, key1), zero);
  EXPECT_THAT(archive.GetStorage(0, addr, key2), zero);
  EXPECT_THAT(archive.GetStorage(0, addr, key3), zero);

  EXPECT_THAT(archive.GetStorage(1, addr, key1), one);
  EXPECT_THAT(archive.GetStorage(1, addr, key2), two);
  EXPECT_THAT(archive.GetStorage(1, addr, key3), zero);

  EXPECT_THAT(archive.GetStorage(2, addr, key1), one);
  EXPECT_THAT(archive.GetStorage(2, addr, key2), two);
  EXPECT_THAT(archive.GetStorage(2, addr, key3), zero);

  EXPECT_THAT(archive.GetStorage(3, addr, key1), two);
  EXPECT_THAT(archive.GetStorage(3, addr, key2), zero);
  EXPECT_THAT(archive.GetStorage(3, addr, key3), one);

  EXPECT_THAT(archive.GetStorage(4, addr, key1), two);
  EXPECT_THAT(archive.GetStorage(4, addr, key2), zero);
  EXPECT_THAT(archive.GetStorage(4, addr, key3), one);
}

TEST(Archive, BlockZeroCanBeAdded) {
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto archive, Archive::Open(dir));

  Update update;
  EXPECT_OK(archive.Add(0, update));
}

TEST(Archive, IncreasingBlockNumbersCanBeAdded) {
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto archive, Archive::Open(dir));

  Update update;
  EXPECT_OK(archive.Add(0, update));
  EXPECT_OK(archive.Add(1, update));
  EXPECT_OK(archive.Add(2, update));
  EXPECT_OK(archive.Add(10, update));
}

TEST(Archive, RepeatedBlockNumbersCanNotBeAdded) {
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto archive, Archive::Open(dir));

  Update update;
  EXPECT_OK(archive.Add(0, update));
  EXPECT_THAT(
      archive.Add(0, update),
      StatusIs(
          _,
          HasSubstr(
              "Unable to insert block 0, archive already contains block 0")));
}

TEST(Archive, BlocksCanNotBeAddedOutOfOrder) {
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto archive, Archive::Open(dir));

  Update update;
  EXPECT_OK(archive.Add(0, update));
  EXPECT_OK(archive.Add(2, update));
  EXPECT_THAT(
      archive.Add(1, update),
      StatusIs(
          _,
          HasSubstr(
              "Unable to insert block 1, archive already contains block 2")));
}

TEST(Archive, InitialAccountHashIsZero) {
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto archive, Archive::Open(dir));
  Address addr1{0x01};
  Address addr2{0x02};
  Hash zero{};
  EXPECT_THAT(archive.GetAccountHash(0, addr1), zero);
  EXPECT_THAT(archive.GetAccountHash(0, addr2), zero);
  EXPECT_THAT(archive.GetAccountHash(4, addr1), zero);
  EXPECT_THAT(archive.GetAccountHash(8, addr2), zero);
}

TEST(Archive, AccountListIncludesAllTouchedAccounts) {
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto archive, Archive::Open(dir));
  Address addr1{0x01};
  Address addr2{0x02};
  Balance balance{0x10};

  Update update1;
  update1.Create(addr1);

  Update update3;
  update3.Create(addr2);

  Update update5;
  update5.Delete(addr1);

  EXPECT_OK(archive.Add(1, update1));
  EXPECT_OK(archive.Add(3, update3));
  EXPECT_OK(archive.Add(5, update5));

  EXPECT_THAT(archive.GetAccountList(0), IsOkAndHolds(ElementsAre()));
  EXPECT_THAT(archive.GetAccountList(1), IsOkAndHolds(ElementsAre(addr1)));
  EXPECT_THAT(archive.GetAccountList(2), IsOkAndHolds(ElementsAre(addr1)));
  EXPECT_THAT(archive.GetAccountList(3),
              IsOkAndHolds(ElementsAre(addr1, addr2)));
  EXPECT_THAT(archive.GetAccountList(4),
              IsOkAndHolds(ElementsAre(addr1, addr2)));
  EXPECT_THAT(archive.GetAccountList(5),
              IsOkAndHolds(ElementsAre(addr1, addr2)));
  EXPECT_THAT(archive.GetAccountList(6),
              IsOkAndHolds(ElementsAre(addr1, addr2)));
}

TEST(Archive, AccountHashesChainUp) {
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto archive, Archive::Open(dir));
  Address addr1{0x01};
  Address addr2{0x02};
  Balance balance{0x10};

  Hash zero{};

  Update update1;
  update1.Create(addr1);

  Update update3;
  update3.Create(addr2);
  update3.Set(addr2, balance);

  Update update5;
  update5.Set(addr1, balance);

  EXPECT_OK(archive.Add(1, update1));
  EXPECT_OK(archive.Add(3, update3));
  EXPECT_OK(archive.Add(5, update5));

  auto hash_update_1 = AccountUpdate::From(update1)[addr1].GetHash();
  auto hash_update_3 = AccountUpdate::From(update3)[addr2].GetHash();
  auto hash_update_5 = AccountUpdate::From(update5)[addr1].GetHash();

  auto hash_account1_b1 = GetSha256Hash(zero, hash_update_1);
  auto hash_account2_b3 = GetSha256Hash(zero, hash_update_3);
  auto hash_account1_b5 = GetSha256Hash(hash_account1_b1, hash_update_5);

  EXPECT_THAT(archive.GetAccountHash(0, addr1), zero);
  EXPECT_THAT(archive.GetAccountHash(0, addr2), zero);

  EXPECT_THAT(archive.GetAccountHash(1, addr1), hash_account1_b1);
  EXPECT_THAT(archive.GetAccountHash(1, addr2), zero);

  EXPECT_THAT(archive.GetAccountHash(2, addr1), hash_account1_b1);
  EXPECT_THAT(archive.GetAccountHash(2, addr2), zero);

  EXPECT_THAT(archive.GetAccountHash(3, addr1), hash_account1_b1);
  EXPECT_THAT(archive.GetAccountHash(3, addr2), hash_account2_b3);

  EXPECT_THAT(archive.GetAccountHash(4, addr1), hash_account1_b1);
  EXPECT_THAT(archive.GetAccountHash(4, addr2), hash_account2_b3);

  EXPECT_THAT(archive.GetAccountHash(5, addr1), hash_account1_b5);
  EXPECT_THAT(archive.GetAccountHash(5, addr2), hash_account2_b3);

  EXPECT_THAT(archive.GetAccountHash(6, addr1), hash_account1_b5);
  EXPECT_THAT(archive.GetAccountHash(6, addr2), hash_account2_b3);
}

TEST(Archive, AccountValidationPassesOnIncrementalUpdates) {
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto archive, Archive::Open(dir));
  Address addr1{0x1};
  Address addr2{0x2};
  Balance balance1{0x1};
  Balance balance2{0x2};
  Nonce nonce1{0x1};
  Nonce nonce2{0x2};
  Key key{0x1};

  Update update1;
  update1.Create(addr1);
  update1.Set(addr1, balance1);
  update1.Set(addr1, nonce1);

  Update update3;
  update3.Create(addr2);
  update3.Set(addr2, balance2);

  Update update5;
  update5.Set(addr1, balance2);
  update5.Set(addr1, nonce2);
  update5.Set(addr1, Code{0x01, 0x02});
  update5.Set(addr1, key, Value{0x01});

  EXPECT_OK(archive.Add(1, update1));
  EXPECT_OK(archive.Add(3, update3));
  EXPECT_OK(archive.Add(5, update5));

  EXPECT_OK(archive.VerifyAccount(0, addr1));
  EXPECT_OK(archive.VerifyAccount(1, addr1));
  EXPECT_OK(archive.VerifyAccount(2, addr1));
  EXPECT_OK(archive.VerifyAccount(3, addr1));
  EXPECT_OK(archive.VerifyAccount(4, addr1));
  EXPECT_OK(archive.VerifyAccount(5, addr1));
  EXPECT_OK(archive.VerifyAccount(6, addr1));

  EXPECT_OK(archive.VerifyAccount(0, addr2));
  EXPECT_OK(archive.VerifyAccount(1, addr2));
  EXPECT_OK(archive.VerifyAccount(2, addr2));
  EXPECT_OK(archive.VerifyAccount(3, addr2));
  EXPECT_OK(archive.VerifyAccount(4, addr2));
  EXPECT_OK(archive.VerifyAccount(5, addr2));
  EXPECT_OK(archive.VerifyAccount(6, addr2));
}

// TODO: check corrupted DB files

TEST(Archive, ArchiveHashIsHashOfAccountHashes) {
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto archive, Archive::Open(dir));
  Address addr1{0x1};
  Address addr2{0x2};
  Balance balance1{0x1};
  Balance balance2{0x2};
  Nonce nonce1{0x1};
  Nonce nonce2{0x2};
  Key key{0x1};

  Update update1;
  update1.Create(addr1);
  update1.Set(addr1, balance1);
  update1.Set(addr1, nonce1);

  Update update3;
  update3.Create(addr2);
  update3.Set(addr2, balance2);

  Update update5;
  update5.Set(addr1, balance2);
  update5.Set(addr1, nonce2);
  update5.Set(addr1, Code{0x01, 0x02});
  update5.Set(addr1, key, Value{0x01});

  EXPECT_OK(archive.Add(1, update1));
  EXPECT_OK(archive.Add(3, update3));
  EXPECT_OK(archive.Add(5, update5));

  for (BlockId block = 0; block <= 6; block++) {
    ASSERT_OK_AND_ASSIGN(auto addr1_hash, archive.GetAccountHash(block, addr1));
    ASSERT_OK_AND_ASSIGN(auto addr2_hash, archive.GetAccountHash(block, addr2));
    ASSERT_OK_AND_ASSIGN(auto archive_hash, archive.GetHash(block));
    if (block < 1) {
      EXPECT_EQ(archive_hash, GetSha256Hash());
    } else if (block < 3) {
      EXPECT_EQ(archive_hash, GetSha256Hash(addr1_hash));
    } else {
      EXPECT_EQ(archive_hash, GetSha256Hash(addr1_hash, addr2_hash));
    }
  }
}

TEST(Archive, ArchiveCanBeVerifiedForCustomBlockHeight) {
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto archive, Archive::Open(dir));
  Address addr1{0x1};
  Address addr2{0x2};
  Balance balance1{0x1};
  Balance balance2{0x2};
  Nonce nonce1{0x1};
  Nonce nonce2{0x2};
  Key key{0x1};

  Update update1;
  update1.Create(addr1);
  update1.Set(addr1, balance1);
  update1.Set(addr1, nonce1);

  Update update3;
  update3.Create(addr2);
  update3.Set(addr2, balance2);

  Update update5;
  update5.Set(addr1, balance2);
  update5.Set(addr1, nonce2);
  update5.Set(addr1, Code{0x01, 0x02});
  update5.Set(addr1, key, Value{0x01});

  EXPECT_OK(archive.Add(1, update1));
  EXPECT_OK(archive.Add(3, update3));
  EXPECT_OK(archive.Add(5, update5));

  for (BlockId block = 0; block <= 6; block++) {
    ASSERT_OK_AND_ASSIGN(auto archive_hash, archive.GetHash(block));
    EXPECT_OK(archive.Verify(block, archive_hash));
  }
}

}  // namespace
}  // namespace carmen
