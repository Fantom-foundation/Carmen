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

#include "backend/index/test_util.h"

#include "backend/index/index.h"
#include "gtest/gtest.h"

namespace carmen::backend::index {
namespace {

// The generic index tests are not explicitly tested here.
GTEST_ALLOW_UNINSTANTIATED_PARAMETERIZED_TEST(IndexTest);

// Check that the MockIndexWrapper implementation is a valid Index.
TEST(MockIndexWrapperTest, IsIndex) {
  EXPECT_TRUE((Index<MockIndex<int, int>>));
}

}  // namespace
}  // namespace carmen::backend::index
