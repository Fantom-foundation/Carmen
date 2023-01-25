#include "state/update.h"

#include <type_traits>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace carmen {
namespace {

using ::testing::IsEmpty;

TEST(Update, IntialUpdateIsEmpty) {
    Update update;
    EXPECT_THAT(update.GetStorage(), IsEmpty());
}

}  // namespace
}  // namespace carmen
