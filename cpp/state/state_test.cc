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
