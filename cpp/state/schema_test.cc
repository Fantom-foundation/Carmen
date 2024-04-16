/*
 * Copyright (c) 2024 Fantom Foundation
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at fantom.foundation/bsl11.
 *
 * Change Date: 2028-4-16
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by the GNU Lesser General Public Licence v3 
 */

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
  EXPECT_THAT(PrintToString(Schema(F::kAddressId)), "{address_id}");
  EXPECT_THAT(PrintToString(Schema(F::kKeyId)), "{key_id}");
  EXPECT_THAT(PrintToString(F::kKeyId & F::kAccountReincarnation),
              "{key_id,account_reincarnation}");
}

TEST(Schema, FeaturesHaveSetSemantic) {
  EXPECT_EQ(Schema(), Schema());

  EXPECT_EQ(Schema(F::kKeyId), Schema(F::kKeyId));
  EXPECT_EQ(Schema(F::kKeyId), Schema(F::kKeyId, F::kKeyId));

  EXPECT_EQ(Schema(F::kAddressId, F::kKeyId), Schema(F::kKeyId, F::kAddressId));

  EXPECT_NE(Schema(), Schema(F::kKeyId));
}

TEST(Schema, CanBeCombined) {
  EXPECT_EQ(Schema(F::kAddressId, F::kKeyId), F::kAddressId & F::kKeyId);

  Schema s;
  EXPECT_EQ(s, s);
  EXPECT_EQ(s & F::kKeyId, Schema(F::kKeyId));
  EXPECT_EQ(s & F::kKeyId & F::kKeyId, Schema(F::kKeyId));
}

}  // namespace
}  // namespace carmen
