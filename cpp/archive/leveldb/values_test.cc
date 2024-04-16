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

#include "archive/leveldb/values.h"

#include <span>

#include "common/status_test_util.h"
#include "common/type.h"
#include "gtest/gtest.h"

namespace carmen::archive::leveldb {
namespace {

TEST(AccountState, ReincarnationNumberCanBeEncodedAndDecoded) {
  AccountState state;
  for (ReincarnationNumber i = 1; i < (ReincarnationNumber(1) << 31); i <<= 1) {
    state.reincarnation_number = i;
    state.exists = !state.exists;
    auto encoded = state.Encode();
    ASSERT_OK_AND_ASSIGN(auto restored, AccountState::From(encoded));
    EXPECT_EQ(state.exists, restored.exists);
    EXPECT_EQ(state.reincarnation_number, restored.reincarnation_number);
  }
}

}  // namespace
}  // namespace carmen::archive::leveldb
