#include "state/update.h"

#include <type_traits>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen {
namespace {

using ::testing::ElementsAre;
using ::testing::FieldsAre;
using ::testing::IsEmpty;

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

}  // namespace
}  // namespace carmen
