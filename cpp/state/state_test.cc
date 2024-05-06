// Copyright (c) 2024 Fantom Foundation
// 
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
// 
// Change Date: 2028-4-16
// 
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

#include "archive/leveldb/archive.h"
#include "state/configurations.h"
//#include "state/state_test_suite.h"

namespace carmen {
namespace {

// The role of this test suite is to test the exported configurations. Specific
// schemas are tested in their respective test suites.

/*
using TestArchive = archive::leveldb::LevelDbArchive;

using StateConfigurations =
    ::testing::Types<InMemoryState<TestArchive>, FileBasedState<TestArchive>,
                     LevelDbBasedState<TestArchive>>;

INSTANTIATE_TYPED_TEST_SUITE_P(Config, StateTest, StateConfigurations);
*/

}  // namespace
}  // namespace carmen
