#include "backend/index/memory/index.h"

#include "backend/index/index_test_suite.h"
#include "gtest/gtest.h"

namespace carmen::backend::index {
namespace {

using TestIndex = InMemoryIndex<int, int>;

// Instantiates common index tests for the InMemory index type.
INSTANTIATE_TYPED_TEST_SUITE_P(InMemory, IndexTest, TestIndex);

}  // namespace
}  // namespace carmen::backend::index
