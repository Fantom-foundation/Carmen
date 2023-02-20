#include "archive/leveldb/archive.h"

#include "archive/archive_test_suite.h"
#include "archive/leveldb/keys.h"
#include "backend/common/leveldb/leveldb.h"
#include "gtest/gtest.h"

namespace carmen::archive::leveldb {
namespace {

using ::carmen::backend::LevelDb;

// Instantiates common archive tests for the LevelDB implementation.
INSTANTIATE_TYPED_TEST_SUITE_P(LevelDbTest, ArchiveTest, LevelDbArchive);

template <typename Check>
void TestCorruption(absl::FunctionRef<void(LevelDb& db)> change,
                    const Check& check) {
  TempDir dir;
  Address addr{0x01};
  Hash hash;
  // Initialize an account with a bit of history.
  {
    ASSERT_OK_AND_ASSIGN(auto archive, LevelDbArchive::Open(dir));
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
    EXPECT_OK(archive.Close());
  }
  // Allow the test case to mess with the DB.
  {
    ASSERT_OK_AND_ASSIGN(auto db, LevelDb::Open(dir));
    change(db);
    ASSERT_OK(db.Close());
  }
  // Reopen the archive and make sure the issue is detected.
  {
    ASSERT_OK_AND_ASSIGN(auto archive, LevelDbArchive::Open(dir));
    check(archive, hash);
  }
}

void TestAccountCorruption(absl::FunctionRef<void(LevelDb& db)> change,
                           std::string_view error = "") {
  TestCorruption(change, [&](LevelDbArchive& archive, const Hash&) {
    EXPECT_THAT(archive.VerifyAccount(10, Address{0x01}),
                StatusIs(_, HasSubstr(error)));
  });
}

TEST(LevelDbArchive, AccountVerificationDetectsMissingHash) {
  TestAccountCorruption(
      [](LevelDb& db) {
        ASSERT_OK(db.Delete(GetAccountHashKey(Address{0x01}, 3)));
      },
      "Archive contains update for block 3 but no hash for it.");
}

TEST(LevelDbArchive, AccountVerificationDetectsModifiedStatusUpdate) {
  TestAccountCorruption(
      [](LevelDb& db) {
        ASSERT_OK(db.Add({GetAccountKey(Address{0x01}, 1),
                          AccountState{false, 1}.Encode()}));
      },
      "Hash for diff at block 1 does not match.");
}

TEST(LevelDbArchive, AccountVerificationDetectsAdditionalStatusUpdate) {
  TestAccountCorruption(
      [](LevelDb& db) {
        ASSERT_OK(db.Add(
            {GetAccountKey(Address{0x01}, 2), AccountState{true, 2}.Encode()}));
      },
      "Archive contains update for block 2 but no hash for it.");
}

TEST(LevelDbArchive, AccountVerificationDetectsModifiedReincarnationNumber) {
  TestAccountCorruption(
      [](LevelDb& db) {
        ASSERT_OK(db.Add(
            {GetAccountKey(Address{0x01}, 1), AccountState{true, 2}.Encode()}));
      },
      "Reincarnation numbers are not incremental");
}

TEST(LevelDbArchive, AccountVerificationDetectsMissingStatusUpdate) {
  TestAccountCorruption(
      [](LevelDb& db) {
        ASSERT_OK(db.Delete(GetAccountKey(Address{0x01}, 3)));
      },
      "Invalid reincarnation number for storage value at block 3, expected 1, "
      "got 2");
}

TEST(LevelDbArchive, AccountVerificationDetectsMissingBalanceUpdate) {
  TestAccountCorruption(
      [](LevelDb& db) {
        ASSERT_OK(db.Delete(GetBalanceKey(Address{0x01}, 1)));
      },
      "Hash for diff at block 1 does not match.");
}

TEST(LevelDbArchive, AccountVerificationDetectsModifiedBalanceUpdate) {
  TestAccountCorruption(
      [](LevelDb& db) {
        ASSERT_OK(db.Add({GetBalanceKey(Address{0x01}, 3), Balance{0xFF}}));
      },
      "Hash for diff at block 3 does not match.");
}

TEST(LevelDbArchive, AccountVerificationDetectsAdditionalBalanceUpdate) {
  TestAccountCorruption(
      [](LevelDb& db) {
        ASSERT_OK(db.Add({GetBalanceKey(Address{0x01}, 4), Balance{0xFF}}));
      },
      "Archive contains update for block 4 but no hash for it.");
}
/*
TEST(LevelDbArchive, AccountVerificationDetectsMissingNonceUpdate) {
  TestAccountCorruption(
      [](LevelDb& db) { ASSERT_OK(db.Run("DELETE FROM nonce WHERE true")); },
      "Hash for diff at block 1 does not match.");
}

TEST(LevelDbArchive, AccountVerificationDetectsModifiedNonceUpdate) {
  TestAccountCorruption(
      [](LevelDb& db) {
        ASSERT_OK(
            db.Run("UPDATE nonce SET value = ? WHERE block = 3", Nonce{0xFF}));
      },
      "Hash for diff at block 3 does not match.");
}

TEST(LevelDbArchive, AccountVerificationDetectsAdditionalNonceUpdate) {
  TestAccountCorruption(
      [](LevelDb& db) {
        ASSERT_OK(
            db.Run("INSERT INTO nonce(account,block,value) VALUES (?,4,?)",
                   Address{0x01}, Nonce{0xFF}));
      },
      "Archive contains update for block 4 but no hash for it.");
}

TEST(LevelDbArchive, AccountVerificationDetectsMissingCodeUpdate) {
  TestAccountCorruption(
      [](LevelDb& db) { ASSERT_OK(db.Run("DELETE FROM code WHERE true")); },
      "Hash for diff at block 1 does not match.");
}

TEST(LevelDbArchive, AccountVerificationDetectsModifiedCodeUpdate) {
  TestAccountCorruption(
      [](LevelDb& db) {
        ASSERT_OK(
            db.Run("UPDATE code SET code = ? WHERE block = 3", Code{0xFF}));
      },
      "Hash for diff at block 3 does not match.");
}

TEST(LevelDbArchive, AccountVerificationDetectsAdditionalCodeUpdate) {
  TestAccountCorruption(
      [](LevelDb& db) {
        ASSERT_OK(db.Run("INSERT INTO code(account,block,code) VALUES (?,4,?)",
                         Address{0x01}, Code{0xFF}));
      },
      "Archive contains update for block 4 but no hash for it.");
}

TEST(LevelDbArchive, AccountVerificationDetectsMissingStorageUpdate) {
  TestAccountCorruption(
      [](LevelDb& db) { ASSERT_OK(db.Run("DELETE FROM storage WHERE true")); },
      "Hash for diff at block 1 does not match.");
}

TEST(LevelDbArchive, AccountVerificationDetectsModifiedStorageUpdate) {
  TestAccountCorruption(
      [](LevelDb& db) {
        ASSERT_OK(
            db.Run("UPDATE storage SET slot = ? WHERE block = 3", Key{0xFF}));
      },
      "Hash for diff at block 3 does not match.");

  TestAccountCorruption(
      [](LevelDb& db) {
        ASSERT_OK(db.Run("UPDATE storage SET value = ? WHERE block = 3",
                         Value{0xFF}));
      },
      "Hash for diff at block 3 does not match.");

  TestAccountCorruption(
      [](LevelDb& db) {
        ASSERT_OK(
            db.Run("UPDATE storage SET reincarnation = 2 WHERE block = 3"));
      },
      "Invalid reincarnation number for storage value at block 3, expected 1, "
      "got 2");
}

TEST(LevelDbArchive, AccountVerificationDetectsAdditionalStorageUpdate) {
  TestAccountCorruption(
      [](LevelDb& db) {
        ASSERT_OK(db.Run(
            "INSERT INTO storage(account,reincarnation,block,slot,value) "
            "VALUES (?,1,4,?,?)",
            Address{0x01}, Key{0xAB}, Value{0xCD}));
      },
      "Archive contains update for block 4 but no hash for it.");

  TestAccountCorruption(
      [](LevelDb& db) {
        ASSERT_OK(db.Run(
            "INSERT INTO storage(account,reincarnation,block,slot,value) "
            "VALUES (?,1,3,?,?)",
            Address{0x01}, Key{0xAB}, Value{0xCD}));
      },
      "Hash for diff at block 3 does not match.");
}

void TestArchiveCorruption(absl::FunctionRef<void(Sqlite& db)> change,
                           std::string_view error = "") {
  TestCorruption(change, [&](SqliteArchive& archive, const Hash& hash) {
    EXPECT_THAT(archive.Verify(10, hash), StatusIs(_, HasSubstr(error)));
  });
}

TEST(LevelDbArchive, VerificationDetectsMissingHash) {
  // Delete a most-recent account update.
  TestArchiveCorruption(
      [](LevelDb& db) {
        ASSERT_OK(db.Run("DELETE FROM account_hash WHERE block = 5"));
      },
      "Validation of hash of block 5 failed.");

  // Delete a historic account update hash.
  TestArchiveCorruption(
      [](LevelDb& db) {
        ASSERT_OK(db.Run("DELETE FROM account_hash WHERE block = 3"));
      },
      "Validation of hash of block 3 failed.");
}

TEST(LevelDbArchive, VerificationDetectsModifiedHashes) {
  // A corrupted hash for a most-recent account update.
  TestArchiveCorruption(
      [](LevelDb& db) {
        ASSERT_OK(
            db.Run("UPDATE account_hash SET hash = ? WHERE block = 5", Hash{}));
      },
      "Validation of hash of block 5 failed.");

  // A corrupted hash for a past account update.
  TestArchiveCorruption(
      [](LevelDb& db) {
        ASSERT_OK(
            db.Run("UPDATE account_hash SET hash = ? WHERE block = 3", Hash{}));
      },
      "Validation of hash of block 3 failed.");
}

TEST(LevelDbArchive, VerificationDetectsAdditionalHashes) {
  // An addition hash representing the most recent update.
  TestArchiveCorruption(
      [](LevelDb& db) {
        ASSERT_OK(db.Run(
            "INSERT INTO account_hash(account,block,hash) VALUES (?,7,?)",
            Address{0x01}, Hash{}));
      },
      "Found change in block 7 not covered by archive hash.");

  // An additional hash somewhere in the history.
  TestArchiveCorruption(
      [](LevelDb& db) {
        ASSERT_OK(db.Run(
            "INSERT INTO account_hash(account,block,hash) VALUES (?,4,?)",
            Address{0x01}, Hash{}));
      },
      "Found account update for block 4 but no hash for this block.");
}

TEST(LevelDbArchive, VerificationDetectsExtraAccountStatus) {
  // An entry in the past with uncovered address.
  TestArchiveCorruption(
      [](LevelDb& db) {
        ASSERT_OK(
            db.Run("INSERT INTO status(account,block,exist,reincarnation) "
                   "VALUES (?,1,0,0)",
                   Address{0x02}));
      },
      "Found extra row of data in table `status`.");

  // An entry in the future.
  TestArchiveCorruption(
      [](LevelDb& db) {
        ASSERT_OK(
            db.Run("INSERT INTO status(account,block,exist,reincarnation) "
                   "VALUES (?,20,0,0)",
                   Address{0x01}));
      },
      "Found entry of future block height in `status`.");
}

TEST(LevelDbArchive, VerificationDetectsExtraBalance) {
  // An entry in the past with uncovered address.
  TestArchiveCorruption(
      [](LevelDb& db) {
        ASSERT_OK(
            db.Run("INSERT INTO balance(account,block,value) VALUES (?,1,?)",
                   Address{0x02}, Balance{}));
      },
      "Found extra row of data in table `balance`.");

  // An entry in the future.
  TestArchiveCorruption(
      [](LevelDb& db) {
        ASSERT_OK(
            db.Run("INSERT INTO balance(account,block,value) VALUES (?,20,?)",
                   Address{0x01}, Balance{}));
      },
      "Found entry of future block height in `balance`.");
}

TEST(LevelDbArchive, VerificationDetectsExtraNonce) {
  // An entry in the past with uncovered address.
  TestArchiveCorruption(
      [](LevelDb& db) {
        ASSERT_OK(
            db.Run("INSERT INTO nonce(account,block,value) VALUES (?,1,?)",
                   Address{0x02}, Nonce{}));
      },
      "Found extra row of data in table `nonce`.");

  // An entry in the future.
  TestArchiveCorruption(
      [](LevelDb& db) {
        ASSERT_OK(
            db.Run("INSERT INTO nonce(account,block,value) VALUES (?,20,?)",
                   Address{0x01}, Nonce{}));
      },
      "Found entry of future block height in `nonce`.");
}

TEST(LevelDbArchive, VerificationDetectsExtraCode) {
  // An entry in the past with uncovered address.
  TestArchiveCorruption(
      [](LevelDb& db) {
        ASSERT_OK(db.Run("INSERT INTO code(account,block,code) VALUES (?,1,?)",
                         Address{0x02}, Code{}));
      },
      "Found extra row of data in table `code`.");

  // An entry in the future.
  TestArchiveCorruption(
      [](LevelDb& db) {
        ASSERT_OK(db.Run("INSERT INTO code(account,block,code) VALUES (?,20,?)",
                         Address{0x01}, Code{}));
      },
      "Found entry of future block height in `code`.");
}

TEST(LevelDbArchive, VerificationDetectsExtraStorage) {
  // An entry in the past with uncovered address.
  TestArchiveCorruption(
      [](LevelDb& db) {
        ASSERT_OK(db.Run(
            "INSERT INTO storage(account,reincarnation,block,slot,value) "
            "VALUES (?,1,1,?,?)",
            Address{0x02}, Key{}, Value{}));
      },
      "Found extra row of data in table `storage`.");

  // An entry in the future.
  TestArchiveCorruption(
      [](LevelDb& db) {
        ASSERT_OK(db.Run(
            "INSERT INTO storage(account,reincarnation,block,slot,value) "
            "VALUES (?,1,20,?,?)",
            Address{0x01}, Key{}, Value{}));
      },
      "Found entry of future block height in `storage`.");
}

TEST(LevelDbArchive, VerificationDetectsCorruptedAccount) {
  // Account verification is tested with its own set of tests. Here we only test
  // that account verification is indeed involved in state validation.
  TestArchiveCorruption(
      [](LevelDb& db) {
        ASSERT_OK(db.Run("UPDATE balance SET value = ? WHERE block = 3",
                         Balance{0xFF}));
      },
      "Hash for diff at block 3 does not match.");
}

TEST(LevelDbArchive, HashOfEmptyArchiveIsZero) {
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto archive, SqliteArchive::Open(dir));
  EXPECT_THAT(archive.GetHash(0), Hash{});
  EXPECT_THAT(archive.GetHash(5), Hash{});
}

TEST(LevelDbArchive, ArchiveHashIsHashOfAccountDiffHashesChain) {
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto archive, SqliteArchive::Open(dir));
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
  update3.Set(addr1, balance2);
  update3.Set(addr2, balance2);

  Update update5;
  update5.Set(addr1, balance1);
  update5.Set(addr1, nonce2);
  update5.Set(addr1, Code{0x01, 0x02});
  update5.Set(addr1, key, Value{0x01});

  EXPECT_OK(archive.Add(1, update1));
  EXPECT_OK(archive.Add(3, update3));
  EXPECT_OK(archive.Add(5, update5));

  ASSERT_OK_AND_ASSIGN(auto hash11, archive.GetAccountHash(1, addr1));
  ASSERT_OK_AND_ASSIGN(auto hash31, archive.GetAccountHash(3, addr1));
  ASSERT_OK_AND_ASSIGN(auto hash32, archive.GetAccountHash(3, addr2));
  ASSERT_OK_AND_ASSIGN(auto hash51, archive.GetAccountHash(5, addr1));

  Hash hash{};
  EXPECT_THAT(archive.GetHash(0), hash);

  hash = GetSha256Hash(hash, hash11);
  EXPECT_THAT(archive.GetHash(1), hash);
  EXPECT_THAT(archive.GetHash(2), hash);

  hash = GetSha256Hash(hash, hash31, hash32);
  EXPECT_THAT(archive.GetHash(3), hash);
  EXPECT_THAT(archive.GetHash(4), hash);

  hash = GetSha256Hash(hash, hash51);
  EXPECT_THAT(archive.GetHash(5), hash);
  EXPECT_THAT(archive.GetHash(6), hash);
}

TEST(LevelDbArchive, ArchiveCanBeVerifiedForCustomBlockHeight) {
  TempDir dir;
  ASSERT_OK_AND_ASSIGN(auto archive, SqliteArchive::Open(dir));
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
*/

}  // namespace
}  // namespace carmen::archive::leveldb
