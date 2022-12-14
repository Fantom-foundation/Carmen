
#include "benchmark/benchmark.h"
#include "common/benchmark.h"
#include "common/file_util.h"
#include "common/hash.h"
#include "common/status_test_util.h"
#include "state/configurations.h"

namespace carmen::backend::store {
namespace {

// To run benchmarks, use the following command:
//    bazel run -c opt //state:state_benchmark

// Defines the list of configurations to be benchmarked.
BENCHMARK_TYPE_LIST(StateConfigList, InMemoryState, FileBasedState,
                    LevelDbBasedState);

// Benchmarks the time it takes to open and close a state DB.
template <typename State>
void BM_OpenClose(benchmark::State& state) {
  TempDir dir;
  for (auto _ : state) {
    ASSERT_OK_AND_ASSIGN(auto state, State::Open(dir));
    ASSERT_OK(state.Close());
  }
}

BENCHMARK_ALL(BM_OpenClose, StateConfigList);

}  // namespace
}  // namespace carmen::backend::store
