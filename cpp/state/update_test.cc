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
  EXPECT_THAT(update.GetDeletedAccounts(), IsEmpty());
  EXPECT_THAT(update.GetCreatedAccounts(), IsEmpty());
  EXPECT_THAT(update.GetBalances(), IsEmpty());
  EXPECT_THAT(update.GetNonces(), IsEmpty());
  EXPECT_THAT(update.GetCodes(), IsEmpty());
  EXPECT_THAT(update.GetStorage(), IsEmpty());
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
  std::vector<std::byte> data(1 + 6 * 2);
  data[0] = std::byte{12};
  EXPECT_THAT(Update::FromBytes(data),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Invalid version number")));
}

TEST(Update, OutOfBoundsCheckIsDetected) {
  std::vector<std::byte> data(1 + 6 * 2);
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
      "0xdd46c3eebb1884ff3b5258c0a2fc9398e560a29e0780d4b53869b6254aa46a96");
  ASSERT_OK_AND_ASSIGN(auto example, GetExampleUpdate().GetHash());
  EXPECT_THAT(
      Print(example),
      "0xbc283c81ee1607c83e557420bf3763ab99aca2a59a99d0c66d7105e1ff2fea26");
}

}  // namespace
}  // namespace carmen
