#include "state/archive.h"

#include <type_traits>

#include "backend/common/sqlite/sqlite.h"
#include "common/file_util.h"
#include "common/hash.h"
#include "common/status_test_util.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen {
namespace {

using ::carmen::backend::Sqlite;
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

TEST(Archive, AccountValidationCanHandleBlockZeroUpdate) {
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto archive, Archive::Open(dir));
  Address addr1{0x1};

  Update update0;
  update0.Create(addr1);

  Update update1;
  update1.Set(addr1, Balance{});

  EXPECT_OK(archive.Add(0, update0));
  EXPECT_OK(archive.Add(1, update1));

  EXPECT_OK(archive.VerifyAccount(0, addr1));
  EXPECT_OK(archive.VerifyAccount(1, addr1));
  EXPECT_OK(archive.VerifyAccount(2, addr1));
}

template <typename Check>
void TestCorruption(absl::FunctionRef<void(Sqlite& db)> change,
                    const Check& check) {
  TempDir dir;
  Address addr{0x01};
  Hash hash;
  // Initialize an account with a bit of history.
  {
    ASSERT_OK_AND_ASSIGN(auto archive, Archive::Open(dir));
    Update update1;
    update1.Create(addr);
    update1.Set(addr, Balance{0x12});
    update1.Set(addr, Nonce{0x13});
    update1.Set(addr, Code{0x14});
    update1.Set(addr, Key{0x15}, Value{0x16});
    EXPECT_OK(archive.Add(1, update1));

    Update update3;
    update3.Delete(addr);
    update3.Set(addr, Balance{0x31});
    update3.Set(addr, Nonce{0x33});
    update3.Set(addr, Code{0x34});
    update3.Set(addr, Key{0x35}, Value{0x36});
    EXPECT_OK(archive.Add(3, update3));

    Update update5;
    update5.Create(addr);
    update5.Set(addr, Balance{0x51});
    EXPECT_OK(archive.Add(5, update5));

    for (BlockId i = 0; i < 10; i++) {
      EXPECT_OK(archive.VerifyAccount(i, addr));
    }

    ASSERT_OK_AND_ASSIGN(hash, archive.GetHash(10));
    EXPECT_OK(archive.Verify(10, hash));
  }
  // Allow the test case to mess with the DB.
  {
    ASSERT_OK_AND_ASSIGN(auto db,
                         Sqlite::Open(dir.GetPath() / "archive.sqlite"));
    change(db);
    ASSERT_OK(db.Close());
  }
  // Reopen the archive and make sure the issue is detected.
  {
    ASSERT_OK_AND_ASSIGN(auto archive, Archive::Open(dir));
    check(archive, hash);
  }
}

void TestAccountCorruption(absl::FunctionRef<void(Sqlite& db)> change,
                           std::string_view error = "") {
  TestCorruption(change, [&](Archive& archive, const Hash&) {
    EXPECT_THAT(archive.VerifyAccount(10, Address{0x01}),
                StatusIs(_, HasSubstr(error)));
  });
}

TEST(Archive, AccountVerificationDetectsMissingHash) {
  TestAccountCorruption(
      [](Sqlite& db) {
        ASSERT_OK(db.Run("DELETE FROM account_hash WHERE block = 3"));
      },
      "Archive contains update for block 3 but no hash for it.");
}

TEST(Archive, AccountVerificationDetectsModifiedStatusUpdate) {
  TestAccountCorruption(
      [](Sqlite& db) { ASSERT_OK(db.Run("UPDATE status SET exist = 0")); },
      "Hash for diff at block 1 does not match.");
}

TEST(Archive, AccountVerificationDetectsAdditionalStatusUpdate) {
  TestAccountCorruption(
      [](Sqlite& db) {
        ASSERT_OK(
            db.Run("INSERT INTO status(account, block, exist,reincarnation) "
                   "VALUES (?,2,1,1)",
                   Address{0x01}));
      },
      "Archive contains update for block 2 but no hash for it.");
}

TEST(Archive, AccountVerificationDetectsModifiedReincarnationNumber) {
  TestAccountCorruption(
      [](Sqlite& db) {
        ASSERT_OK(db.Run("UPDATE status SET reincarnation = 0"));
      },
      "Reincarnation numbers are not incremental");
}

TEST(Archive, AccountVerificationDetectsMissingStatusUpdate) {
  TestAccountCorruption(
      [](Sqlite& db) {
        ASSERT_OK(db.Run("DELETE FROM status WHERE block = 3"));
      },
      "Invalid reincarnation number for storage value at block 3, expected 0, "
      "got 1");
}

TEST(Archive, AccountVerificationDetectsMissingBalanceUpdate) {
  TestAccountCorruption(
      [](Sqlite& db) { ASSERT_OK(db.Run("DELETE FROM balance WHERE true")); },
      "Hash for diff at block 1 does not match.");
}

TEST(Archive, AccountVerificationDetectsModifiedBalanceUpdate) {
  TestAccountCorruption(
      [](Sqlite& db) {
        ASSERT_OK(db.Run("UPDATE balance SET value = ? WHERE block = 3",
                         Balance{0xFF}));
      },
      "Hash for diff at block 3 does not match.");
}

TEST(Archive, AccountVerificationDetectsAdditionalBalanceUpdate) {
  TestAccountCorruption(
      [](Sqlite& db) {
        ASSERT_OK(
            db.Run("INSERT INTO balance(account,block,value) VALUES (?,4,?)",
                   Address{0x01}, Balance{0xFF}));
      },
      "Archive contains update for block 4 but no hash for it.");
}

TEST(Archive, AccountVerificationDetectsMissingNonceUpdate) {
  TestAccountCorruption(
      [](Sqlite& db) { ASSERT_OK(db.Run("DELETE FROM nonce WHERE true")); },
      "Hash for diff at block 1 does not match.");
}

TEST(Archive, AccountVerificationDetectsModifiedNonceUpdate) {
  TestAccountCorruption(
      [](Sqlite& db) {
        ASSERT_OK(
            db.Run("UPDATE nonce SET value = ? WHERE block = 3", Nonce{0xFF}));
      },
      "Hash for diff at block 3 does not match.");
}

TEST(Archive, AccountVerificationDetectsAdditionalNonceUpdate) {
  TestAccountCorruption(
      [](Sqlite& db) {
        ASSERT_OK(
            db.Run("INSERT INTO nonce(account,block,value) VALUES (?,4,?)",
                   Address{0x01}, Nonce{0xFF}));
      },
      "Archive contains update for block 4 but no hash for it.");
}

TEST(Archive, AccountVerificationDetectsMissingCodeUpdate) {
  TestAccountCorruption(
      [](Sqlite& db) { ASSERT_OK(db.Run("DELETE FROM code WHERE true")); },
      "Hash for diff at block 1 does not match.");
}

TEST(Archive, AccountVerificationDetectsModifiedCodeUpdate) {
  TestAccountCorruption(
      [](Sqlite& db) {
        ASSERT_OK(
            db.Run("UPDATE code SET code = ? WHERE block = 3", Code{0xFF}));
      },
      "Hash for diff at block 3 does not match.");
}

TEST(Archive, AccountVerificationDetectsAdditionalCodeUpdate) {
  TestAccountCorruption(
      [](Sqlite& db) {
        ASSERT_OK(db.Run("INSERT INTO code(account,block,code) VALUES (?,4,?)",
                         Address{0x01}, Code{0xFF}));
      },
      "Archive contains update for block 4 but no hash for it.");
}

TEST(Archive, AccountVerificationDetectsMissingStorageUpdate) {
  TestAccountCorruption(
      [](Sqlite& db) { ASSERT_OK(db.Run("DELETE FROM storage WHERE true")); },
      "Hash for diff at block 1 does not match.");
}

TEST(Archive, AccountVerificationDetectsModifiedStorageUpdate) {
  TestAccountCorruption(
      [](Sqlite& db) {
        ASSERT_OK(
            db.Run("UPDATE storage SET slot = ? WHERE block = 3", Key{0xFF}));
      },
      "Hash for diff at block 3 does not match.");

  TestAccountCorruption(
      [](Sqlite& db) {
        ASSERT_OK(db.Run("UPDATE storage SET value = ? WHERE block = 3",
                         Value{0xFF}));
      },
      "Hash for diff at block 3 does not match.");

  TestAccountCorruption(
      [](Sqlite& db) {
        ASSERT_OK(
            db.Run("UPDATE storage SET reincarnation = 2 WHERE block = 3"));
      },
      "Invalid reincarnation number for storage value at block 3, expected 1, "
      "got 2");
}

TEST(Archive, AccountVerificationDetectsAdditionalStorageUpdate) {
  TestAccountCorruption(
      [](Sqlite& db) {
        ASSERT_OK(db.Run(
            "INSERT INTO storage(account,reincarnation,block,slot,value) "
            "VALUES (?,1,4,?,?)",
            Address{0x01}, Key{0xAB}, Value{0xCD}));
      },
      "Archive contains update for block 4 but no hash for it.");

  TestAccountCorruption(
      [](Sqlite& db) {
        ASSERT_OK(db.Run(
            "INSERT INTO storage(account,reincarnation,block,slot,value) "
            "VALUES (?,1,3,?,?)",
            Address{0x01}, Key{0xAB}, Value{0xCD}));
      },
      "Hash for diff at block 3 does not match.");
}

void TestArchiveCorruption(absl::FunctionRef<void(Sqlite& db)> change,
                           std::string_view error = "") {
  TestCorruption(change, [&](Archive& archive, const Hash& hash) {
    EXPECT_THAT(archive.Verify(10, hash), StatusIs(_, HasSubstr(error)));
  });
}

TEST(Archive, VerificationDetectsMissingHash) {
  // Delete a most-recent account update.
  TestArchiveCorruption(
      [](Sqlite& db) {
        ASSERT_OK(db.Run("DELETE FROM account_hash WHERE block = 5"));
      },
      "Archive hash does not match expected hash.");

  // Delete a historic account update hash.
  TestArchiveCorruption(
      [](Sqlite& db) {
        ASSERT_OK(db.Run("DELETE FROM account_hash WHERE block = 3"));
      },
      "Archive contains update for block 3 but no hash for it.");
}

TEST(Archive, VerificationDetectsModifiedHashes) {
  // A corrupted hash for a most-recent account update.
  TestArchiveCorruption(
      [](Sqlite& db) {
        ASSERT_OK(
            db.Run("UPDATE account_hash SET hash = ? WHERE block = 5", Hash{}));
      },
      "Archive hash does not match expected hash.");

  // A corrupted hash for a past account update.
  TestArchiveCorruption(
      [](Sqlite& db) {
        ASSERT_OK(
            db.Run("UPDATE account_hash SET hash = ? WHERE block = 3", Hash{}));
      },
      "Hash for diff at block 3 does not match.");
}

TEST(Archive, VerificationDetectsAdditionalHashes) {
  // An addition hash representing the most recent update.
  TestArchiveCorruption(
      [](Sqlite& db) {
        ASSERT_OK(db.Run(
            "INSERT INTO account_hash(account,block,hash) VALUES (?,7,?)",
            Address{0x01}, Hash{}));
      },
      "Archive hash does not match expected hash.");

  // An additional hash somewhere in the history.
  TestArchiveCorruption(
      [](Sqlite& db) {
        ASSERT_OK(db.Run(
            "INSERT INTO account_hash(account,block,hash) VALUES (?,4,?)",
            Address{0x01}, Hash{}));
      },
      "Archive contains hash for update at block 4 but no change for it.");
}

TEST(Archive, VerificationDetectsExtraAccountStatus) {
  // An entry in the past with uncovered address.
  TestArchiveCorruption(
      [](Sqlite& db) {
        ASSERT_OK(
            db.Run("INSERT INTO status(account,block,exist,reincarnation) "
                   "VALUES (?,1,0,0)",
                   Address{0x02}));
      },
      "Found extra row of data in table `status`.");

  // An entry in the future.
  TestArchiveCorruption(
      [](Sqlite& db) {
        ASSERT_OK(
            db.Run("INSERT INTO status(account,block,exist,reincarnation) "
                   "VALUES (?,20,0,0)",
                   Address{0x01}));
      },
      "Found entry of future block height in `status`.");
}

TEST(Archive, VerificationDetectsExtraBalance) {
  // An entry in the past with uncovered address.
  TestArchiveCorruption(
      [](Sqlite& db) {
        ASSERT_OK(
            db.Run("INSERT INTO balance(account,block,value) VALUES (?,1,?)",
                   Address{0x02}, Balance{}));
      },
      "Found extra row of data in table `balance`.");

  // An entry in the future.
  TestArchiveCorruption(
      [](Sqlite& db) {
        ASSERT_OK(
            db.Run("INSERT INTO balance(account,block,value) VALUES (?,20,?)",
                   Address{0x01}, Balance{}));
      },
      "Found entry of future block height in `balance`.");
}

TEST(Archive, VerificationDetectsExtraNonce) {
  // An entry in the past with uncovered address.
  TestArchiveCorruption(
      [](Sqlite& db) {
        ASSERT_OK(
            db.Run("INSERT INTO nonce(account,block,value) VALUES (?,1,?)",
                   Address{0x02}, Nonce{}));
      },
      "Found extra row of data in table `nonce`.");

  // An entry in the future.
  TestArchiveCorruption(
      [](Sqlite& db) {
        ASSERT_OK(
            db.Run("INSERT INTO nonce(account,block,value) VALUES (?,20,?)",
                   Address{0x01}, Nonce{}));
      },
      "Found entry of future block height in `nonce`.");
}

TEST(Archive, VerificationDetectsExtraCode) {
  // An entry in the past with uncovered address.
  TestArchiveCorruption(
      [](Sqlite& db) {
        ASSERT_OK(db.Run("INSERT INTO code(account,block,code) VALUES (?,1,?)",
                         Address{0x02}, Code{}));
      },
      "Found extra row of data in table `code`.");

  // An entry in the future.
  TestArchiveCorruption(
      [](Sqlite& db) {
        ASSERT_OK(db.Run("INSERT INTO code(account,block,code) VALUES (?,20,?)",
                         Address{0x01}, Code{}));
      },
      "Found entry of future block height in `code`.");
}

TEST(Archive, VerificationDetectsExtraStorage) {
  // An entry in the past with uncovered address.
  TestArchiveCorruption(
      [](Sqlite& db) {
        ASSERT_OK(db.Run(
            "INSERT INTO storage(account,reincarnation,block,slot,value) "
            "VALUES (?,1,1,?,?)",
            Address{0x02}, Key{}, Value{}));
      },
      "Found extra row of data in table `storage`.");

  // An entry in the future.
  TestArchiveCorruption(
      [](Sqlite& db) {
        ASSERT_OK(db.Run(
            "INSERT INTO storage(account,reincarnation,block,slot,value) "
            "VALUES (?,1,20,?,?)",
            Address{0x01}, Key{}, Value{}));
      },
      "Found entry of future block height in `storage`.");
}

TEST(Archive, VerificationDetectsCorruptedAccount) {
  // Account verification is tested with its own set of tests. Here we only test
  // that account verification is indeed involved in state validation.
  TestArchiveCorruption(
      [](Sqlite& db) {
        ASSERT_OK(db.Run("UPDATE balance SET value = ? WHERE block = 3",
                         Balance{0xFF}));
      },
      "Hash for diff at block 3 does not match.");
}

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
