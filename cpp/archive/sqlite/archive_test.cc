// Copyright (c) 2024 Fantom Foundation
// 
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
// 
// Change Date: 2028-4-16
// 
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

#include "archive/sqlite/archive.h"

#include <type_traits>

#include "archive/archive.h"
#include "archive/archive_test_suite.h"
#include "backend/common/sqlite/sqlite.h"
#include "common/file_util.h"
#include "common/hash.h"
#include "common/status_test_util.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen::archive::sqlite {
namespace {

using ::carmen::backend::Sqlite;
using ::testing::_;
using ::testing::HasSubstr;
using ::testing::StatusIs;

// Instantiates common archive tests for the SQLite implementation.
INSTANTIATE_TYPED_TEST_SUITE_P(SqliteTest, ArchiveTest, SqliteArchive);

template <typename Check>
void TestCorruption(absl::FunctionRef<void(Sqlite& db)> change,
                    const Check& check) {
  TempDir dir;
  Address addr{0x01};
  Hash hash;
  // Initialize an account with a bit of history.
  {
    ASSERT_OK_AND_ASSIGN(auto archive, SqliteArchive::Open(dir));
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
    ASSERT_OK_AND_ASSIGN(auto archive, SqliteArchive::Open(dir));
    check(archive, hash);
  }
}

void TestAccountCorruption(absl::FunctionRef<void(Sqlite& db)> change,
                           std::string_view error = "") {
  TestCorruption(change, [&](SqliteArchive& archive, const Hash&) {
    EXPECT_THAT(archive.VerifyAccount(10, Address{0x01}),
                StatusIs(_, HasSubstr(error)));
  });
}

TEST(SqliteArchive, AccountVerificationDetectsMissingHash) {
  TestAccountCorruption(
      [](Sqlite& db) {
        ASSERT_OK(db.Run("DELETE FROM account_hash WHERE block = 3"));
      },
      "Archive contains update for block 3 but no hash for it.");
}

TEST(SqliteArchive, AccountVerificationDetectsModifiedStatusUpdate) {
  TestAccountCorruption(
      [](Sqlite& db) { ASSERT_OK(db.Run("UPDATE status SET exist = 0")); },
      "Hash for diff at block 1 does not match.");
}

TEST(SqliteArchive, AccountVerificationDetectsAdditionalStatusUpdate) {
  TestAccountCorruption(
      [](Sqlite& db) {
        ASSERT_OK(
            db.Run("INSERT INTO status(account, block, exist,reincarnation) "
                   "VALUES (?,2,1,1)",
                   Address{0x01}));
      },
      "Archive contains update for block 2 but no hash for it.");
}

TEST(SqliteArchive, AccountVerificationDetectsModifiedReincarnationNumber) {
  TestAccountCorruption(
      [](Sqlite& db) {
        ASSERT_OK(db.Run("UPDATE status SET reincarnation = 0"));
      },
      "Reincarnation numbers are not incremental");
}

TEST(SqliteArchive, AccountVerificationDetectsMissingStatusUpdate) {
  TestAccountCorruption(
      [](Sqlite& db) {
        ASSERT_OK(db.Run("DELETE FROM status WHERE block = 3"));
      },
      "Invalid reincarnation number for storage value at block 3, expected 0, "
      "got 1");
}

TEST(SqliteArchive, AccountVerificationDetectsMissingBalanceUpdate) {
  TestAccountCorruption(
      [](Sqlite& db) { ASSERT_OK(db.Run("DELETE FROM balance WHERE true")); },
      "Hash for diff at block 1 does not match.");
}

TEST(SqliteArchive, AccountVerificationDetectsModifiedBalanceUpdate) {
  TestAccountCorruption(
      [](Sqlite& db) {
        ASSERT_OK(db.Run("UPDATE balance SET value = ? WHERE block = 3",
                         Balance{0xFF}));
      },
      "Hash for diff at block 3 does not match.");
}

TEST(SqliteArchive, AccountVerificationDetectsAdditionalBalanceUpdate) {
  TestAccountCorruption(
      [](Sqlite& db) {
        ASSERT_OK(
            db.Run("INSERT INTO balance(account,block,value) VALUES (?,4,?)",
                   Address{0x01}, Balance{0xFF}));
      },
      "Archive contains update for block 4 but no hash for it.");
}

TEST(SqliteArchive, AccountVerificationDetectsMissingNonceUpdate) {
  TestAccountCorruption(
      [](Sqlite& db) { ASSERT_OK(db.Run("DELETE FROM nonce WHERE true")); },
      "Hash for diff at block 1 does not match.");
}

TEST(SqliteArchive, AccountVerificationDetectsModifiedNonceUpdate) {
  TestAccountCorruption(
      [](Sqlite& db) {
        ASSERT_OK(
            db.Run("UPDATE nonce SET value = ? WHERE block = 3", Nonce{0xFF}));
      },
      "Hash for diff at block 3 does not match.");
}

TEST(SqliteArchive, AccountVerificationDetectsAdditionalNonceUpdate) {
  TestAccountCorruption(
      [](Sqlite& db) {
        ASSERT_OK(
            db.Run("INSERT INTO nonce(account,block,value) VALUES (?,4,?)",
                   Address{0x01}, Nonce{0xFF}));
      },
      "Archive contains update for block 4 but no hash for it.");
}

TEST(SqliteArchive, AccountVerificationDetectsMissingCodeUpdate) {
  TestAccountCorruption(
      [](Sqlite& db) { ASSERT_OK(db.Run("DELETE FROM code WHERE true")); },
      "Hash for diff at block 1 does not match.");
}

TEST(SqliteArchive, AccountVerificationDetectsModifiedCodeUpdate) {
  TestAccountCorruption(
      [](Sqlite& db) {
        ASSERT_OK(
            db.Run("UPDATE code SET code = ? WHERE block = 3", Code{0xFF}));
      },
      "Hash for diff at block 3 does not match.");
}

TEST(SqliteArchive, AccountVerificationDetectsAdditionalCodeUpdate) {
  TestAccountCorruption(
      [](Sqlite& db) {
        ASSERT_OK(db.Run("INSERT INTO code(account,block,code) VALUES (?,4,?)",
                         Address{0x01}, Code{0xFF}));
      },
      "Archive contains update for block 4 but no hash for it.");
}

TEST(SqliteArchive, AccountVerificationDetectsMissingStorageUpdate) {
  TestAccountCorruption(
      [](Sqlite& db) { ASSERT_OK(db.Run("DELETE FROM storage WHERE true")); },
      "Hash for diff at block 1 does not match.");
}

TEST(SqliteArchive, AccountVerificationDetectsModifiedStorageUpdate) {
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

TEST(SqliteArchive, AccountVerificationDetectsAdditionalStorageUpdate) {
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
  TestCorruption(change, [&](SqliteArchive& archive, const Hash& hash) {
    EXPECT_THAT(archive.Verify(10, hash), StatusIs(_, HasSubstr(error)));
  });
}

TEST(SqliteArchive, VerificationDetectsMissingHash) {
  // Delete a most-recent account update.
  TestArchiveCorruption(
      [](Sqlite& db) {
        ASSERT_OK(db.Run("DELETE FROM account_hash WHERE block = 5"));
      },
      "Validation of hash of block 5 failed.");

  // Delete a historic account update hash.
  TestArchiveCorruption(
      [](Sqlite& db) {
        ASSERT_OK(db.Run("DELETE FROM account_hash WHERE block = 3"));
      },
      "Validation of hash of block 3 failed.");
}

TEST(SqliteArchive, VerificationDetectsModifiedHashes) {
  // A corrupted hash for a most-recent account update.
  TestArchiveCorruption(
      [](Sqlite& db) {
        ASSERT_OK(
            db.Run("UPDATE account_hash SET hash = ? WHERE block = 5", Hash{}));
      },
      "Validation of hash of block 5 failed.");

  // A corrupted hash for a past account update.
  TestArchiveCorruption(
      [](Sqlite& db) {
        ASSERT_OK(
            db.Run("UPDATE account_hash SET hash = ? WHERE block = 3", Hash{}));
      },
      "Validation of hash of block 3 failed.");
}

TEST(SqliteArchive, VerificationDetectsAdditionalHashes) {
  // An addition hash representing the most recent update.
  TestArchiveCorruption(
      [](Sqlite& db) {
        ASSERT_OK(db.Run(
            "INSERT INTO account_hash(account,block,hash) VALUES (?,7,?)",
            Address{0x01}, Hash{}));
      },
      "Found change in block 7 not covered by archive hash.");

  // An additional hash somewhere in the history.
  TestArchiveCorruption(
      [](Sqlite& db) {
        ASSERT_OK(db.Run(
            "INSERT INTO account_hash(account,block,hash) VALUES (?,4,?)",
            Address{0x01}, Hash{}));
      },
      "Found account update for block 4 but no hash for this block.");
}

TEST(SqliteArchive, VerificationDetectsExtraAccountStatus) {
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

TEST(SqliteArchive, VerificationDetectsExtraBalance) {
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

TEST(SqliteArchive, VerificationDetectsExtraNonce) {
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

TEST(SqliteArchive, VerificationDetectsExtraCode) {
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

TEST(SqliteArchive, VerificationDetectsExtraStorage) {
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

TEST(SqliteArchive, VerificationDetectsCorruptedAccount) {
  // Account verification is tested with its own set of tests. Here we only test
  // that account verification is indeed involved in state validation.
  TestArchiveCorruption(
      [](Sqlite& db) {
        ASSERT_OK(db.Run("UPDATE balance SET value = ? WHERE block = 3",
                         Balance{0xFF}));
      },
      "Hash for diff at block 3 does not match.");
}

}  // namespace
}  // namespace carmen::archive::sqlite
