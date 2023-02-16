#include "state/update.h"

#include <type_traits>

#include "common/hash.h"
#include "common/status_test_util.h"
#include "common/test_util.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen {
namespace {

using ::testing::_;
using ::testing::ElementsAre;
using ::testing::FieldsAre;
using ::testing::HasSubstr;
using ::testing::IsEmpty;
using ::testing::IsOk;
using ::testing::Not;
using ::testing::StatusIs;
using ::testing::StrEq;

TEST(Update, IntialUpdateIsEmpty) {
  Update update;
  EXPECT_TRUE(update.Empty());
  EXPECT_THAT(update.GetDeletedAccounts(), IsEmpty());
  EXPECT_THAT(update.GetCreatedAccounts(), IsEmpty());
  EXPECT_THAT(update.GetBalances(), IsEmpty());
  EXPECT_THAT(update.GetNonces(), IsEmpty());
  EXPECT_THAT(update.GetCodes(), IsEmpty());
  EXPECT_THAT(update.GetStorage(), IsEmpty());
}

TEST(Update, DeletedAccountsAreVisible) {
  Address addr1{0x01};
  Address addr2{0x02};

  Update update;
  EXPECT_THAT(update.GetDeletedAccounts(), ElementsAre());
  update.Delete(addr1);
  EXPECT_THAT(update.GetDeletedAccounts(), ElementsAre(addr1));
  update.Delete(addr2);
  EXPECT_THAT(update.GetDeletedAccounts(), ElementsAre(addr1, addr2));
  EXPECT_FALSE(update.Empty());
}

TEST(Update, CreatedAccountsAreVisible) {
  Address addr1{0x01};
  Address addr2{0x02};

  Update update;
  EXPECT_THAT(update.GetCreatedAccounts(), ElementsAre());
  update.Create(addr1);
  EXPECT_THAT(update.GetCreatedAccounts(), ElementsAre(addr1));
  update.Create(addr2);
  EXPECT_THAT(update.GetCreatedAccounts(), ElementsAre(addr1, addr2));
  EXPECT_FALSE(update.Empty());
}

TEST(Update, AddedBalancesAreVisible) {
  Address addr1{0x01};
  Address addr2{0x02};
  Balance one{0x01};
  Balance two{0x02};

  Update update;
  EXPECT_THAT(update.GetBalances(), ElementsAre());
  update.Set(addr1, one);
  EXPECT_THAT(update.GetBalances(), ElementsAre(FieldsAre(addr1, one)));
  update.Set(addr2, two);
  EXPECT_THAT(update.GetBalances(),
              ElementsAre(FieldsAre(addr1, one), FieldsAre(addr2, two)));
  EXPECT_FALSE(update.Empty());
}

TEST(Update, AddedCodesAreVisible) {
  Address addr1{0x01};
  Address addr2{0x02};
  Code one{0x01};
  Code two{0x02, 0x03};

  Update update;
  EXPECT_THAT(update.GetCodes(), ElementsAre());
  update.Set(addr1, one);
  EXPECT_THAT(update.GetCodes(), ElementsAre(FieldsAre(addr1, one)));
  update.Set(addr2, two);
  EXPECT_THAT(update.GetCodes(),
              ElementsAre(FieldsAre(addr1, one), FieldsAre(addr2, two)));
  EXPECT_FALSE(update.Empty());
}

TEST(Update, AddedNoncesAreVisible) {
  Address addr1{0x01};
  Address addr2{0x02};
  Nonce one{0x01};
  Nonce two{0x02};

  Update update;
  EXPECT_THAT(update.GetNonces(), ElementsAre());
  update.Set(addr1, one);
  EXPECT_THAT(update.GetNonces(), ElementsAre(FieldsAre(addr1, one)));
  update.Set(addr2, two);
  EXPECT_THAT(update.GetNonces(),
              ElementsAre(FieldsAre(addr1, one), FieldsAre(addr2, two)));
  EXPECT_FALSE(update.Empty());
}

TEST(Update, AddedStorageUpdatesAreVisible) {
  Address addr1{0x01};
  Address addr2{0x02};
  Key key1{0x01};
  Key key2{0x02};
  Value one{0x01};
  Value two{0x02};

  Update update;
  EXPECT_THAT(update.GetStorage(), ElementsAre());
  update.Set(addr1, key1, one);
  EXPECT_THAT(update.GetStorage(), ElementsAre(FieldsAre(addr1, key1, one)));
  update.Set(addr2, key2, two);
  EXPECT_THAT(update.GetStorage(), ElementsAre(FieldsAre(addr1, key1, one),
                                               FieldsAre(addr2, key2, two)));
  EXPECT_FALSE(update.Empty());
}

TEST(Update, EmptyDataCanBeSerializedAndRestored) {
  std::vector<std::byte> data;
  {
    Update update;
    ASSERT_OK_AND_ASSIGN(data, update.ToBytes());
  }
  {
    ASSERT_OK_AND_ASSIGN(auto restored, Update::FromBytes(data));
    EXPECT_EQ(restored, Update());
  }
}

Update GetExampleUpdate() {
  Update update;
  update.Delete(Address{0xA1});
  update.Delete(Address{0xA2});

  update.Create(Address{0xB1});
  update.Create(Address{0xB2});
  update.Create(Address{0xB3});

  update.Set(Address{0xC1}, Balance{0x01});
  update.Set(Address{0xC2}, Balance{0x02});

  update.Set(Address{0xD1}, Nonce{0x03});
  update.Set(Address{0xD2}, Nonce{0x04});

  update.Set(Address{0xE1}, Code{});
  update.Set(Address{0xE2}, Code{0x01});
  update.Set(Address{0xE3}, Code{0x02, 0x03});

  update.Set(Address{0xF1}, Key{0x01}, Value{0xA1});
  update.Set(Address{0xF2}, Key{0x02}, Value{0xA2});
  update.Set(Address{0xF3}, Key{0x03}, Value{0xB1});
  return update;
}

TEST(Update, NonEmptyUpdateCanBeSerializedAndRestored) {
  auto update = GetExampleUpdate();
  ASSERT_OK_AND_ASSIGN(auto data, update.ToBytes());
  ASSERT_OK_AND_ASSIGN(auto restored, Update::FromBytes(data));
  EXPECT_EQ(restored, update);
}

TEST(Update, ParsingEmptyDataFailsWithError) {
  EXPECT_THAT(Update::FromBytes({}),
              StatusIs(absl::StatusCode::kInvalidArgument, _));
}

TEST(Update, InvalidVersionNumberIsDetected) {
  std::vector<std::byte> data(1 + 6 * 4);
  data[0] = std::byte{12};
  EXPECT_THAT(Update::FromBytes(data),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Invalid version number")));
}

TEST(Update, OutOfBoundsCheckIsDetected) {
  std::vector<std::byte> data(1 + 6 * 4);
  data[3] = std::byte{12};  // = 12 deleted accounts
  EXPECT_THAT(
      Update::FromBytes(data),
      StatusIs(absl::StatusCode::kInvalidArgument, HasSubstr("end of data")));
}

TEST(Update, TruncatedInputIsHandledSafely) {
  Update update = GetExampleUpdate();
  ASSERT_OK_AND_ASSIGN(auto data, update.ToBytes());
  for (std::size_t i = 0; i < data.size(); i++) {
    auto span = std::span(data.data(), i);
    EXPECT_THAT(Update::FromBytes(span), Not(IsOk()));
  }
  EXPECT_OK(Update::FromBytes(data));
}

TEST(Update, KnownEncodings) {
  // The hashes for the empty update and the example update are aligned between
  // the C++ and Go version.
  ASSERT_OK_AND_ASSIGN(auto empty, Update().GetHash());
  EXPECT_THAT(
      Print(empty),
      "0x61126de1b795b976f3ac878f48e88fa77a87d7308ba57c7642b9e1068403a496");
  ASSERT_OK_AND_ASSIGN(auto example, GetExampleUpdate().GetHash());
  EXPECT_THAT(
      Print(example),
      "0x5524cf120229bc99ef762303b408900b8998f1d725958e62ac49fea69a5f7901");
}

TEST(AccountUpdate, IsNormalizedDetectsOutOfOrderSlotUpdates) {
  AccountUpdate update;
  update.storage.push_back({Key{0x02}, Value{}});
  EXPECT_OK(update.IsNormalized());
  update.storage.push_back({Key{0x01}, Value{}});
  EXPECT_THAT(update.IsNormalized(),
              StatusIs(absl::StatusCode::kInternal, HasSubstr("not in order")));
}

TEST(AccountUpdate, IsNormalizedDetectsDuplicatedSlotUpdates) {
  AccountUpdate update;
  update.storage.push_back({Key{0x02}, Value{}});
  EXPECT_OK(update.IsNormalized());
  update.storage.push_back({Key{0x02}, Value{}});
  EXPECT_THAT(
      update.IsNormalized(),
      StatusIs(absl::StatusCode::kInternal, HasSubstr("contains collisions")));
}

TEST(AccountUpdate, NormalizeFixesSlotUpdateOrder) {
  using S = AccountUpdate::SlotUpdate;
  AccountUpdate update;
  S s1{Key{0x01}, Value{0x01}};
  S s2{Key{0x02}, Value{0x02}};
  S s3{Key{0x03}, Value{0x03}};
  update.storage.push_back(s2);
  update.storage.push_back(s1);
  update.storage.push_back(s3);
  EXPECT_OK(update.Normalize());
  EXPECT_OK(update.IsNormalized());
  EXPECT_THAT(update.storage, ElementsAre(s1, s2, s3));
}

TEST(AccountUpdate, NormalizeRemovesDuplicates) {
  using S = AccountUpdate::SlotUpdate;
  AccountUpdate update;
  S s1{Key{0x01}, Value{0x01}};
  S s2{Key{0x02}, Value{0x02}};
  S s3{Key{0x03}, Value{0x03}};
  update.storage.push_back(s2);
  update.storage.push_back(s1);
  update.storage.push_back(s3);
  update.storage.push_back(s1);
  update.storage.push_back(s2);
  EXPECT_OK(update.Normalize());
  EXPECT_OK(update.IsNormalized());
  EXPECT_THAT(update.storage, ElementsAre(s1, s2, s3));
}

TEST(AccountUpdate, NormalizeFailsOnCollisions) {
  using S = AccountUpdate::SlotUpdate;
  AccountUpdate update;
  S s2a{Key{0x02}, Value{0x02}};
  S s2b{Key{0x02}, Value{0x03}};
  update.storage.push_back(s2a);
  update.storage.push_back(s2b);
  EXPECT_THAT(update.Normalize(),
              StatusIs(_, HasSubstr("conflicting updates")));
}

TEST(AccountUpdate, HashOfEmptyAccountUpdateIsHashOfEmptyString) {
  AccountUpdate update;
  EXPECT_EQ(update.GetHash(), GetSha256Hash(""));
}

TEST(AccountUpdate, HashOfAccountStateChangesAreHashesOfSingleByte) {
  AccountUpdate update;
  EXPECT_EQ(update.GetHash(), GetSha256Hash(""));
  update.created = true;
  EXPECT_EQ(update.GetHash(), GetSha256Hash(std::uint8_t(1)));
  update.deleted = true;
  EXPECT_EQ(update.GetHash(), GetSha256Hash(std::uint8_t(3)));
  update.created = false;
  EXPECT_EQ(update.GetHash(), GetSha256Hash(std::uint8_t(2)));
}

TEST(AccountUpdate, HashOfBalanceChangeIsHashOfBalance) {
  AccountUpdate update;
  Balance b{0x1, 0x2};
  update.balance = b;
  EXPECT_EQ(update.GetHash(), GetSha256Hash(std::uint8_t(4), b));
}

TEST(AccountUpdate, HashOfNonceChangeIsHashOfBalance) {
  AccountUpdate update;
  Nonce n{0x1, 0x2};
  update.nonce = n;
  EXPECT_EQ(update.GetHash(), GetSha256Hash(std::uint8_t(8), n));
}

TEST(AccountUpdate, HashOfCodeChangeIsHashOfCode) {
  AccountUpdate update;
  Code c{0x1, 0x2, 0x3};
  update.code = c;
  EXPECT_EQ(update.GetHash(),
            GetSha256Hash(std::uint8_t(16), std::uint32_t(c.Size()), c));
}

TEST(AccountUpdate, SlotUpdatesAreHashedInOrder) {
  AccountUpdate update;
  Key k1{0x01};
  Key k2{0x02};
  Value v1{0x10};
  Value v2{0x20};
  update.storage.push_back({k1, v1});
  update.storage.push_back({k2, v2});
  EXPECT_EQ(update.GetHash(), GetSha256Hash(std::uint8_t(0), k1, v1, k2, v2));
}

TEST(AccountUpdate, BlanceNonceCodeAndStorageAreHashedInOrder) {
  AccountUpdate update;
  Balance b{0x1, 0x2};
  Nonce n{0x1, 0x2};
  Code c{0x1, 0x2, 0x3};
  Key k1{0x01};
  Value v1{0x10};
  update.balance = b;
  update.nonce = n;
  update.code = c;
  update.storage.push_back({k1, v1});
  EXPECT_EQ(update.GetHash(),
            GetSha256Hash(std::uint8_t(4 + 8 + 16), b, n,
                          std::uint32_t(c.Size()), c, k1, v1));
}

}  // namespace
}  // namespace carmen
