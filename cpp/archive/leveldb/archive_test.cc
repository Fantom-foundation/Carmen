/*
 * Copyright (c) 2024 Fantom Foundation
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE file and at fantom.foundation/bsl11.
 *
 * Change Date: 2028-4-16
 *
 * On the date above, in accordance with the Business Source License, use of
 * this software will be governed by the GNU Lesser General Public Licence v3.
 */

#include "archive/leveldb/archive.h"

#include "absl/strings/str_format.h"
#include "archive/archive_test_suite.h"
#include "archive/leveldb/keys.h"
#include "archive/leveldb/values.h"
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

TEST(LevelDbArchive, AccountVerificationDetectsMailformedAccountHashKey) {
  // Detects a too short key.
  TestAccountCorruption(
      [](LevelDb& db) {
        auto key = GetAccountHashKey(Address{0x01}, 3);
        ASSERT_OK(db.Delete(key));
        ASSERT_OK(db.Add(std::span(key).subspan(0, key.size() - 1), Hash{}));
      },
      "Invalid key length, expected 25 byte, got 24");

  // Detects a too long key.
  TestAccountCorruption(
      [](LevelDb& db) {
        auto key = GetAccountHashKey(Address{0x01}, 3);
        ASSERT_OK(db.Delete(key));
        std::vector<char> extended(key.begin(), key.end());
        extended.push_back('x');
        ASSERT_OK(db.Add(extended, Hash{}));
      },
      "Invalid key length, expected 25 byte, got 26");
}

TEST(LevelDbArchive, AccountVerificationDetectsMailformedAccountHashValue) {
  // Detects a too short value.
  TestAccountCorruption(
      [](LevelDb& db) {
        ASSERT_OK(db.Add(GetAccountHashKey(Address{0x01}, 3),
                         std::vector<char>(sizeof(Hash) - 1, 'x')));
      },
      "Invalid value length, expected 32 byte, got 31");

  // Detects a too long value.
  TestAccountCorruption(
      [](LevelDb& db) {
        ASSERT_OK(db.Add(GetAccountHashKey(Address{0x01}, 3),
                         std::vector<char>(sizeof(Hash) + 1, 'x')));
      },
      "Invalid value length, expected 32 byte, got 33");
}

TEST(LevelDbArchive, AccountVerificationDetectsModifiedStatusUpdate) {
  TestAccountCorruption(
      [](LevelDb& db) {
        ASSERT_OK(db.Add(GetAccountStateKey(Address{0x01}, 1),
                         AccountState{false, 1}.Encode()));
      },
      "Hash for diff at block 1 does not match.");
}

TEST(LevelDbArchive, AccountVerificationDetectsAdditionalStatusUpdate) {
  TestAccountCorruption(
      [](LevelDb& db) {
        ASSERT_OK(db.Add(GetAccountStateKey(Address{0x01}, 2),
                         AccountState{true, 2}.Encode()));
      },
      "Archive contains update for block 2 but no hash for it.");
}

TEST(LevelDbArchive, AccountVerificationDetectsModifiedReincarnationNumber) {
  TestAccountCorruption(
      [](LevelDb& db) {
        ASSERT_OK(db.Add(GetAccountStateKey(Address{0x01}, 1),
                         AccountState{true, 2}.Encode()));
      },
      "Reincarnation numbers are not incremental");
}

TEST(LevelDbArchive, AccountVerificationDetectsMissingStatusUpdate) {
  TestAccountCorruption(
      [](LevelDb& db) {
        ASSERT_OK(db.Delete(GetAccountStateKey(Address{0x01}, 3)));
      },
      "Invalid reincarnation number for storage value at block 3, expected 1, "
      "got 2");
}

TEST(LevelDbArchive, AccountVerificationDetectsMailformedAccountStatusKey) {
  // Detects a too short key.
  TestAccountCorruption(
      [](LevelDb& db) {
        auto key = GetAccountStateKey(Address{0x01}, 1);
        ASSERT_OK(db.Delete(key));
        ASSERT_OK(db.Add(std::span(key).subspan(0, key.size() - 1), Hash{}));
      },
      "Invalid key length, expected 25 byte, got 24");

  // Detects a too long key.
  TestAccountCorruption(
      [](LevelDb& db) {
        auto key = GetAccountStateKey(Address{0x01}, 1);
        ASSERT_OK(db.Delete(key));
        std::vector<char> extended(key.begin(), key.end());
        extended.push_back('x');
        ASSERT_OK(db.Add(extended, Hash{}));
      },
      "Invalid key length, expected 25 byte, got 26");
}

TEST(LevelDbArchive, AccountVerificationDetectsMailformedAccountStatusValue) {
  static const auto kAccountStateSize = sizeof(AccountState{}.Encode());
  // Detects a too short value.
  TestAccountCorruption(
      [](LevelDb& db) {
        ASSERT_OK(db.Add(GetAccountStateKey(Address{0x01}, 3),
                         std::vector<char>(kAccountStateSize - 1, 'x')));
      },
      "Invalid value length, expected 5 byte, got 4");

  // Detects a too long value.
  TestAccountCorruption(
      [](LevelDb& db) {
        ASSERT_OK(db.Add(GetAccountStateKey(Address{0x01}, 3),
                         std::vector<char>(kAccountStateSize + 1, 'x')));
      },
      "Invalid value length, expected 5 byte, got 6");
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
        ASSERT_OK(db.Add(GetBalanceKey(Address{0x01}, 3), Balance{0xFF}));
      },
      "Hash for diff at block 3 does not match.");
}

TEST(LevelDbArchive, AccountVerificationDetectsAdditionalBalanceUpdate) {
  TestAccountCorruption(
      [](LevelDb& db) {
        ASSERT_OK(db.Add(GetBalanceKey(Address{0x01}, 4), Balance{0xFF}));
      },
      "Archive contains update for block 4 but no hash for it.");
}

TEST(LevelDbArchive, AccountVerificationDetectsMailformedBalanceKey) {
  // Detects a too short key.
  TestAccountCorruption(
      [](LevelDb& db) {
        auto key = GetBalanceKey(Address{0x01}, 1);
        ASSERT_OK(db.Delete(key));
        ASSERT_OK(db.Add(std::span(key).subspan(0, key.size() - 1), Hash{}));
      },
      "Invalid key length, expected 25 byte, got 24");

  // Detects a too long key.
  TestAccountCorruption(
      [](LevelDb& db) {
        auto key = GetBalanceKey(Address{0x01}, 1);
        ASSERT_OK(db.Delete(key));
        std::vector<char> extended(key.begin(), key.end());
        extended.push_back('x');
        ASSERT_OK(db.Add(extended, Hash{}));
      },
      "Invalid key length, expected 25 byte, got 26");
}

TEST(LevelDbArchive, AccountVerificationDetectsMailformedBalanceValue) {
  // Detects a too short value.
  TestAccountCorruption(
      [](LevelDb& db) {
        ASSERT_OK(db.Add(GetBalanceKey(Address{0x01}, 3),
                         std::vector<char>(sizeof(Balance) - 1, 'x')));
      },
      absl::StrFormat("Invalid value length, expected %d byte, got %d",
                      sizeof(Balance), sizeof(Balance) - 1));

  // Detects a too long value.
  TestAccountCorruption(
      [](LevelDb& db) {
        ASSERT_OK(db.Add(GetBalanceKey(Address{0x01}, 3),
                         std::vector<char>(sizeof(Balance) + 1, 'x')));
      },
      absl::StrFormat("Invalid value length, expected %d byte, got %d",
                      sizeof(Balance), sizeof(Balance) + 1));
}

TEST(LevelDbArchive, AccountVerificationDetectsMissingNonceUpdate) {
  TestAccountCorruption(
      [](LevelDb& db) { ASSERT_OK(db.Delete(GetNonceKey(Address{0x01}, 1))); },
      "Hash for diff at block 1 does not match.");
}

TEST(LevelDbArchive, AccountVerificationDetectsModifiedNonceUpdate) {
  TestAccountCorruption(
      [](LevelDb& db) {
        ASSERT_OK(db.Add(GetNonceKey(Address{0x01}, 3), Nonce{0xFF}));
      },
      "Hash for diff at block 3 does not match.");
}

TEST(LevelDbArchive, AccountVerificationDetectsAdditionalNonceUpdate) {
  TestAccountCorruption(
      [](LevelDb& db) {
        ASSERT_OK(db.Add(GetNonceKey(Address{0x01}, 4), Nonce{0xFF}));
      },
      "Archive contains update for block 4 but no hash for it.");
}

TEST(LevelDbArchive, AccountVerificationDetectsMailformedNonceKey) {
  // Detects a too short key.
  TestAccountCorruption(
      [](LevelDb& db) {
        auto key = GetNonceKey(Address{0x01}, 1);
        ASSERT_OK(db.Delete(key));
        ASSERT_OK(db.Add(std::span(key).subspan(0, key.size() - 1), Hash{}));
      },
      "Invalid key length, expected 25 byte, got 24");

  // Detects a too long key.
  TestAccountCorruption(
      [](LevelDb& db) {
        auto key = GetNonceKey(Address{0x01}, 1);
        ASSERT_OK(db.Delete(key));
        std::vector<char> extended(key.begin(), key.end());
        extended.push_back('x');
        ASSERT_OK(db.Add(extended, Hash{}));
      },
      "Invalid key length, expected 25 byte, got 26");
}

TEST(LevelDbArchive, AccountVerificationDetectsMailformedNonceValue) {
  // Detects a too short value.
  TestAccountCorruption(
      [](LevelDb& db) {
        ASSERT_OK(db.Add(GetNonceKey(Address{0x01}, 3),
                         std::vector<char>(sizeof(Nonce) - 1, 'x')));
      },
      absl::StrFormat("Invalid value length, expected %d byte, got %d",
                      sizeof(Nonce), sizeof(Nonce) - 1));

  // Detects a too long value.
  TestAccountCorruption(
      [](LevelDb& db) {
        ASSERT_OK(db.Add(GetNonceKey(Address{0x01}, 3),
                         std::vector<char>(sizeof(Nonce) + 1, 'x')));
      },
      absl::StrFormat("Invalid value length, expected %d byte, got %d",
                      sizeof(Nonce), sizeof(Nonce) + 1));
}

TEST(LevelDbArchive, AccountVerificationDetectsMissingCodeUpdate) {
  TestAccountCorruption(
      [](LevelDb& db) { ASSERT_OK(db.Delete(GetCodeKey(Address{0x01}, 1))); },
      "Hash for diff at block 1 does not match.");
}

TEST(LevelDbArchive, AccountVerificationDetectsModifiedCodeUpdate) {
  TestAccountCorruption(
      [](LevelDb& db) {
        ASSERT_OK(db.Add(GetCodeKey(Address{0x01}, 3), Code{0xFF}));
      },
      "Hash for diff at block 3 does not match.");
}

TEST(LevelDbArchive, AccountVerificationDetectsAdditionalCodeUpdate) {
  TestAccountCorruption(
      [](LevelDb& db) {
        ASSERT_OK(db.Add(GetCodeKey(Address{0x01}, 4), Code{0xFF}));
      },
      "Archive contains update for block 4 but no hash for it.");
}

TEST(LevelDbArchive, AccountVerificationDetectsMailformedCodeKey) {
  // Detects a too short key.
  TestAccountCorruption(
      [](LevelDb& db) {
        auto key = GetCodeKey(Address{0x01}, 1);
        ASSERT_OK(db.Delete(key));
        ASSERT_OK(db.Add(std::span(key).subspan(0, key.size() - 1), Hash{}));
      },
      "Invalid key length, expected 25 byte, got 24");

  // Detects a too long key.
  TestAccountCorruption(
      [](LevelDb& db) {
        auto key = GetCodeKey(Address{0x01}, 1);
        ASSERT_OK(db.Delete(key));
        std::vector<char> extended(key.begin(), key.end());
        extended.push_back('x');
        ASSERT_OK(db.Add(extended, Hash{}));
      },
      "Invalid key length, expected 25 byte, got 26");
}

TEST(LevelDbArchive, AccountVerificationDetectsMissingStorageUpdate) {
  TestAccountCorruption(
      [](LevelDb& db) { ASSERT_OK(db.Delete(GetCodeKey(Address{0x01}, 1))); },
      "Hash for diff at block 1 does not match.");
}

TEST(LevelDbArchive, AccountVerificationDetectsModifiedStorageUpdate) {
  TestAccountCorruption(
      [](LevelDb& db) {
        // Change the key ..
        ASSERT_OK(db.Delete(GetStorageKey(Address{0x01}, 2, Key{0x35}, 3)));
        ASSERT_OK(
            db.Add(GetStorageKey(Address{0x01}, 2, Key{0xFF}, 3), Value{0x36}));
      },
      "Hash for diff at block 3 does not match.");

  TestAccountCorruption(
      [](LevelDb& db) {
        // Change the value
        ASSERT_OK(
            db.Add(GetStorageKey(Address{0x01}, 2, Key{0x35}, 3), Value{0xFF}));
      },
      "Hash for diff at block 3 does not match.");

  TestAccountCorruption(
      [](LevelDb& db) {
        // Change the reincarnation number.
        ASSERT_OK(db.Delete(GetStorageKey(Address{0x01}, 2, Key{0x35}, 3)));
        ASSERT_OK(
            db.Add(GetStorageKey(Address{0x01}, 3, Key{0x35}, 3), Value{0x36}));
      },
      "Invalid reincarnation number for storage value at block 3, expected 2, "
      "got 3");
}

TEST(LevelDbArchive, AccountVerificationDetectsAdditionalStorageUpdate) {
  TestAccountCorruption(
      [](LevelDb& db) {
        ASSERT_OK(
            db.Add(GetStorageKey(Address{0x01}, 2, Key{0xAB}, 4), Value{0xCD}));
      },
      "Archive contains update for block 4 but no hash for it.");

  TestAccountCorruption(
      [](LevelDb& db) {
        ASSERT_OK(
            db.Add(GetStorageKey(Address{0x01}, 2, Key{0xAB}, 3), Value{0xCD}));
      },
      "Hash for diff at block 3 does not match.");
}

TEST(LevelDbArchive, AccountVerificationDetectsMailformedStorageKey) {
  // Detects a too short key.
  TestAccountCorruption(
      [](LevelDb& db) {
        auto key = GetStorageKey(Address{0x01}, 2, Key{0x35}, 3);
        ASSERT_OK(db.Delete(key));
        ASSERT_OK(db.Add(std::span(key).subspan(0, key.size() - 1), Hash{}));
      },
      "Invalid key length, expected 61 byte, got 60");

  // Detects a too long key.
  TestAccountCorruption(
      [](LevelDb& db) {
        auto key = GetStorageKey(Address{0x01}, 2, Key{0x35}, 3);
        ASSERT_OK(db.Delete(key));
        std::vector<char> extended(key.begin(), key.end());
        extended.push_back('x');
        ASSERT_OK(db.Add(extended, Hash{}));
      },
      "Invalid key length, expected 61 byte, got 62");
}

TEST(LevelDbArchive, AccountVerificationDetectsMailformedStorageValue) {
  // Detects a too short value.
  TestAccountCorruption(
      [](LevelDb& db) {
        ASSERT_OK(db.Add(GetStorageKey(Address{0x01}, 2, Key{0x35}, 3),
                         std::vector<char>(sizeof(Value) - 1, 'x')));
      },
      absl::StrFormat("Invalid value length, expected %d byte, got %d",
                      sizeof(Value), sizeof(Value) - 1));

  // Detects a too long value.
  TestAccountCorruption(
      [](LevelDb& db) {
        ASSERT_OK(db.Add(GetStorageKey(Address{0x01}, 2, Key{0x35}, 3),
                         std::vector<char>(sizeof(Value) + 1, 'x')));
      },
      absl::StrFormat("Invalid value length, expected %d byte, got %d",
                      sizeof(Value), sizeof(Value) + 1));
}

void TestArchiveCorruption(absl::FunctionRef<void(LevelDb& db)> change,
                           std::string_view error = "") {
  TestCorruption(change, [&](LevelDbArchive& archive, const Hash& hash) {
    EXPECT_THAT(archive.Verify(10, hash), StatusIs(_, HasSubstr(error)));
  });
}

TEST(LevelDbArchive, VerificationDetectsMissingHash) {
  // Delete a most-recent account update.
  TestArchiveCorruption(
      [](LevelDb& db) {
        ASSERT_OK(db.Delete(GetAccountHashKey(Address{0x01}, 5)));
      },
      "No diff hash found for block 5.");

  // Delete a historic account update hash.
  TestArchiveCorruption(
      [](LevelDb& db) {
        ASSERT_OK(db.Delete(GetAccountHashKey(Address{0x01}, 3)));
      },
      "No diff hash found for block 3.");
}

TEST(LevelDbArchive, VerificationDetectsModifiedHashes) {
  // A corrupted hash for a most-recent account update.
  TestArchiveCorruption(
      [](LevelDb& db) {
        ASSERT_OK(db.Add(GetAccountHashKey(Address{0x01}, 5), Hash{}));
      },
      "Validation of hash of block 5 failed.");

  // A corrupted hash for a past account update.
  TestArchiveCorruption(
      [](LevelDb& db) {
        ASSERT_OK(db.Add(GetAccountHashKey(Address{0x01}, 3), Hash{}));
      },
      "Validation of hash of block 3 failed.");
}

TEST(LevelDbArchive, VerificationDetectsAdditionalHashes) {
  // An addition hash representing the most recent update.
  TestArchiveCorruption(
      [](LevelDb& db) {
        ASSERT_OK(db.Add(GetAccountHashKey(Address{0x01}, 7), Hash{}));
      },
      "Found change in block 7 not covered by archive hash.");

  // An additional hash somewhere in the history.
  TestArchiveCorruption(
      [](LevelDb& db) {
        ASSERT_OK(db.Add(GetAccountHashKey(Address{0x01}, 4), Hash{}));
      },
      "Found account update for block 4 but no hash for this block.");
}

TEST(LevelDbArchive, AccountVerificationDetectsMailformedBlockKey) {
  // Detects a too short key.
  TestArchiveCorruption(
      [](LevelDb& db) {
        auto key = GetBlockKey(3);
        ASSERT_OK(db.Delete(key));
        ASSERT_OK(db.Add(std::span(key).subspan(0, key.size() - 1), Hash{}));
      },
      "Invalid block key length encountered.");

  // Detects a too long key.
  TestArchiveCorruption(
      [](LevelDb& db) {
        auto key = GetBlockKey(3);
        ASSERT_OK(db.Delete(key));
        std::vector<char> extended(key.begin(), key.end());
        extended.push_back('x');
        ASSERT_OK(db.Add(extended, Hash{}));
      },
      "Invalid block key length encountered.");
}

TEST(LevelDbArchive, AccountVerificationDetectsMailformedBlockValue) {
  // Detects a too short value.
  TestArchiveCorruption(
      [](LevelDb& db) {
        ASSERT_OK(
            db.Add(GetBlockKey(3), std::vector<char>(sizeof(Hash) - 1, 'x')));
      },
      "Invalid block value length encountered.");

  // Detects a too long value.
  TestArchiveCorruption(
      [](LevelDb& db) {
        ASSERT_OK(
            db.Add(GetBlockKey(3), std::vector<char>(sizeof(Hash) + 1, 'x')));
      },
      "Invalid block value length encountered.");
}

TEST(LevelDbArchive, VerificationDetectsExtraAccountStatus) {
  // An entry in the past with uncovered address.
  TestArchiveCorruption(
      [](LevelDb& db) {
        ASSERT_OK(db.Add(GetAccountStateKey(Address{0x02}, 1),
                         AccountState{true, 0}.Encode()));
      },
      "Found extra key/value pair in key space `account_state`.");

  // An entry in the future.
  TestArchiveCorruption(
      [](LevelDb& db) {
        ASSERT_OK(db.Add(GetAccountStateKey(Address{0x01}, 20),
                         AccountState{true, 0}.Encode()));
      },
      "Found entry of future block height in key space `account_state`.");
}

TEST(LevelDbArchive, VerificationDetectsExtraBalance) {
  // An entry in the past with uncovered address.
  TestArchiveCorruption(
      [](LevelDb& db) {
        ASSERT_OK(db.Add(GetBalanceKey(Address{0x02}, 1), Balance{}));
      },
      "Found extra key/value pair in key space `balance`.");

  // An entry in the future.
  TestArchiveCorruption(
      [](LevelDb& db) {
        ASSERT_OK(db.Add(GetBalanceKey(Address{0x01}, 20), Balance{}));
      },
      "Found entry of future block height in key space `balance`.");
}

TEST(LevelDbArchive, VerificationDetectsExtraNonce) {
  // An entry in the past with uncovered address.
  TestArchiveCorruption(
      [](LevelDb& db) {
        ASSERT_OK(db.Add(GetNonceKey(Address{0x02}, 1), Nonce{}));
      },
      "Found extra key/value pair in key space `nonce`.");

  // An entry in the future.
  TestArchiveCorruption(
      [](LevelDb& db) {
        ASSERT_OK(db.Add(GetNonceKey(Address{0x01}, 20), Nonce{}));
      },
      "Found entry of future block height in key space `nonce`.");
}

TEST(LevelDbArchive, VerificationDetectsExtraCode) {
  // An entry in the past with uncovered address.
  TestArchiveCorruption(
      [](LevelDb& db) {
        ASSERT_OK(db.Add(GetCodeKey(Address{0x02}, 1), Code{}));
      },
      "Found extra key/value pair in key space `code`.");

  // An entry in the future.
  TestArchiveCorruption(
      [](LevelDb& db) {
        ASSERT_OK(db.Add(GetCodeKey(Address{0x01}, 20), Code{}));
      },
      "Found entry of future block height in key space `code`.");
}

TEST(LevelDbArchive, VerificationDetectsExtraStorage) {
  // An entry in the past with uncovered address.
  TestArchiveCorruption(
      [](LevelDb& db) {
        ASSERT_OK(db.Add(GetStorageKey(Address{0x02}, 2, Key{}, 1), Value{}));
      },
      "Found extra key/value pair in key space `storage`.");

  // An entry in the future.
  TestArchiveCorruption(
      [](LevelDb& db) {
        ASSERT_OK(db.Add(GetStorageKey(Address{0x01}, 2, Key{}, 20), Value{}));
      },
      "Found entry of future block height in key space `storage`.");
}

TEST(LevelDbArchive, VerificationDetectsCorruptedAccount) {
  // Account verification is tested with its own set of tests. Here we only test
  // that account verification is indeed involved in state verification.
  TestArchiveCorruption(
      [](LevelDb& db) {
        ASSERT_OK(db.Add(GetBalanceKey(Address{0x01}, 3), Balance{0xFF}));
      },
      "Hash for diff at block 3 does not match.");
}

}  // namespace
}  // namespace carmen::archive::leveldb
