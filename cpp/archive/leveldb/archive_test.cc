#include "archive/leveldb/archive.h"

#include "archive/archive_test_suite.h"
#include "gtest/gtest.h"

namespace carmen::archive::leveldb {
namespace {

// Instantiates common archive tests for the LevelDB implementation.
INSTANTIATE_TYPED_TEST_SUITE_P(LevelDbTest, ArchiveTest, LevelDbArchive);

// TODO: add data corruption tests

}  // namespace
}  // namespace carmen::archive::leveldb
