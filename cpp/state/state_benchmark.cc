
#include "archive/leveldb/archive.h"
#include "benchmark/benchmark.h"
#include "common/benchmark.h"
#include "common/file_util.h"
#include "common/hash.h"
#include "common/status_test_util.h"
#include "state/configurations.h"
#include "state/s1/state.h"
#include "state/s2/state.h"
#include "state/s3/state.h"

namespace carmen::backend::store {
namespace {

using Archive = archive::leveldb::LevelDbArchive;

// To run benchmarks, use the following command:
//    bazel run -c opt //state:state_benchmark

// Defines the list of configurations to be benchmarked.
BENCHMARK_TYPE_LIST(StateConfigList, (s1::State<InMemoryConfig<Archive>>),
                    (s1::State<FileBasedConfig<Archive>>),
                    (s1::State<LevelDbBasedConfig<Archive>>),

                    (s2::State<InMemoryConfig<Archive>>),
                    (s2::State<FileBasedConfig<Archive>>),
                    (s2::State<LevelDbBasedConfig<Archive>>),

                    (s3::State<InMemoryConfig<Archive>>),
                    (s3::State<FileBasedConfig<Archive>>),
                    (s3::State<LevelDbBasedConfig<Archive>>));

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
