#include "state/schema.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen {
namespace {

using ::testing::PrintToString;

using F = StateFeature;

TEST(Schema, CanBePrinted) {
  Schema schema;
  EXPECT_THAT(PrintToString(schema), "{}");
  EXPECT_THAT(PrintToString(Schema(F::kKeyId)), "{key_id}");
  EXPECT_THAT(PrintToString(F::kKeyId & F::kAccountReincarnation),
              "{key_id,account_reincarnation}");
}

TEST(Schema, FeaturesHaveSetSemantic) {
  EXPECT_EQ(Schema(), Schema());

  EXPECT_EQ(Schema(F::kKeyId), Schema(F::kKeyId));
  EXPECT_EQ(Schema(F::kKeyId), Schema(F::kKeyId, F::kKeyId));

  EXPECT_EQ(Schema(F::kKeyId, F::kAccountReincarnation),
            Schema(F::kAccountReincarnation, F::kKeyId));

  EXPECT_NE(Schema(), Schema(F::kKeyId));
}

TEST(Schema, CanBeCombined) {
  EXPECT_EQ(Schema(F::kKeyId, F::kAccountReincarnation),
            F::kKeyId & F::kAccountReincarnation);

  Schema s;
  EXPECT_EQ(s, s);
  EXPECT_EQ(s & F::kKeyId, Schema(F::kKeyId));
  EXPECT_EQ(s & F::kKeyId & F::kKeyId, Schema(F::kKeyId));
}

TEST(Schema, KeyIdFeatureDoesNotChangeHash) {
  EXPECT_PRED2(ProduceSameHash, Schema(), Schema(F::kKeyId));
}

TEST(Schema, AccountReincarnationFeatureDoesChangeHash) {
  EXPECT_FALSE(ProduceSameHash(Schema(), F::kAccountReincarnation));
}

}  // namespace
}  // namespace carmen
