#include "common/fs_util.h"

#include <fstream>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "common/status_test_util.h"

namespace carmen {
namespace {

using ::testing::StrEq;

TEST(FsUtilTest, TestFsWriteOpen) {
  TempDir dir;
  std::fstream fs;
  ASSERT_OK(fs_open(fs, dir.GetPath(), std::ios::out));
}

}  // namespace
}  // namespace carmen
