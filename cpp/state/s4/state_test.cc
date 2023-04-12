#include "state/s4/state.h"

#include "archive/leveldb/archive.h"
#include "state/configurations.h"
#include "state/state_test_suite.h"

namespace carmen::s4 {
namespace {

// ------------------------- Functionality Tests ------------------------------

using TestArchive = archive::leveldb::LevelDbArchive;

using StateConfigurations = ::testing::Types<State<
    InMemoryConfig<TestArchive>>  //,
                                  // State<FileBasedConfig<TestArchive>>,
                                  // State<LevelDbBasedConfig<TestArchive>>
                                             >;

INSTANTIATE_TYPED_TEST_SUITE_P(Schema_4, StateTest, StateConfigurations);

}  // namespace
}  // namespace carmen::s4
