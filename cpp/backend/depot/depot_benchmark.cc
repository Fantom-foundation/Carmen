#include <random>

#include "backend/depot/depot_handler.h"
#include "backend/depot/file/depot.h"
#include "backend/depot/leveldb/depot.h"
#include "backend/depot/memory/depot.h"
#include "benchmark/benchmark.h"
#include "common/benchmark.h"

namespace carmen::backend::depot {
namespace {

constexpr const std::size_t kBranchFactor = 32;
constexpr const std::size_t kHashBoxSize = 8;
constexpr const auto kInsertValue = std::array<std::byte, 4>{
    std::byte{1}, std::byte{2}, std::byte{3}, std::byte{4}};

// To run benchmarks, use the following command:
//    bazel run -c opt //backend/depot:depot_benchmark

// Defines the list of configurations to be benchmarked.
BENCHMARK_TYPE_LIST(DepotConfigList, InMemoryDepot<unsigned int>,
                    FileDepot<unsigned int>, LevelDBDepot<unsigned int>);

// Defines the list of problem sizes.
const auto kSizes = std::vector<int64_t>({1 << 20, 1 << 24});

// Utility to initialize a depot with a given number of elements.
template <typename Depot>
void InitDepot(Depot& depot, std::size_t num_elements) {
  for (std::size_t i = 0; i < num_elements; i++) {
    auto res = depot.Set(i, kInsertValue);
  }
  auto res = depot.GetHash();
}

// Benchmarks the sequential insertion of keys into depots.
template <typename Depot>
void BM_SequentialInsert(benchmark::State& state) {
  auto num_elements = state.range(0);
  for (auto _ : state) {
    DepotHandler<Depot, kBranchFactor, kHashBoxSize> wrapper;
    auto& depot = wrapper.GetDepot();
    for (int i = 0; i < num_elements; i++) {
      auto res = depot.Set(i, kInsertValue);
    }
  }
}

BENCHMARK_ALL(BM_SequentialInsert, DepotConfigList)->ArgList(kSizes);

// Benchmarks the appending of new elements to the depot.
template <typename Depot>
void BM_Insert(benchmark::State& state) {
  // The size of the depot before the inserts.
  auto num_elements = state.range(0);

  // Initialize the depot with the initial number of elements.
  DepotHandler<Depot, kBranchFactor, kHashBoxSize> wrapper;
  auto& depot = wrapper.GetDepot();
  InitDepot(depot, num_elements);

  // Append additional elements to the end of the depot.
  auto i = num_elements;
  for (auto _ : state) {
    auto res = depot.Set(i++, kInsertValue);
  }
}

BENCHMARK_ALL(BM_Insert, DepotConfigList)->ArgList(kSizes);

// Benchmarks sequential read of keys.
template <typename Depot>
void BM_SequentialRead(benchmark::State& state) {
  auto num_elements = state.range(0);
  DepotHandler<Depot, kBranchFactor, kHashBoxSize> wrapper;

  // Initialize the depot with the total number of elements.
  auto& depot = wrapper.GetDepot();
  InitDepot(depot, num_elements);

  int i = 0;
  for (auto _ : state) {
    auto value = depot.Get(i++ % num_elements);
    benchmark::DoNotOptimize(value);
  }
}

BENCHMARK_ALL(BM_SequentialRead, DepotConfigList)->ArgList(kSizes);

// Benchmarks random, uniformly distributed reads
template <typename Depot>
void BM_UniformRandomRead(benchmark::State& state) {
  auto num_elements = state.range(0);
  DepotHandler<Depot, kBranchFactor, kHashBoxSize> wrapper;

  // Initialize the depot with the total number of elements.
  auto& depot = wrapper.GetDepot();
  InitDepot(depot, num_elements);

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dist(0, num_elements - 1);
  for (auto _ : state) {
    auto value = depot.Get(dist(gen));
    benchmark::DoNotOptimize(value);
  }
}

BENCHMARK_ALL(BM_UniformRandomRead, DepotConfigList)->ArgList(kSizes);

// Benchmarks random, exponentially distributed reads
template <typename Depot>
void BM_ExponentialRandomRead(benchmark::State& state) {
  auto num_elements = state.range(0);
  DepotHandler<Depot, kBranchFactor, kHashBoxSize> wrapper;

  // Initialize the depot with the total number of elements.
  auto& depot = wrapper.GetDepot();
  InitDepot(depot, num_elements);

  std::random_device rd;
  std::mt19937 gen(rd());
  std::exponential_distribution<> dist(double(10) / num_elements);
  for (auto _ : state) {
    auto value = depot.Get(static_cast<std::size_t>(dist(gen)) % num_elements);
    benchmark::DoNotOptimize(value);
  }
}

BENCHMARK_ALL(BM_ExponentialRandomRead, DepotConfigList)->ArgList(kSizes);

// Benchmarks sequential writes of keys.
template <typename Depot>
void BM_SequentialWrite(benchmark::State& state) {
  auto num_elements = state.range(0);
  DepotHandler<Depot, kBranchFactor, kHashBoxSize> wrapper;

  // Initialize the depot with the total number of elements.
  auto& depot = wrapper.GetDepot();
  InitDepot(depot, num_elements);

  int i = 0;
  for (auto _ : state) {
    auto value = std::array<std::byte, 1>{static_cast<std::byte>(i)};
    auto res = depot.Set(i++ % num_elements, value);
  }
}

BENCHMARK_ALL(BM_SequentialWrite, DepotConfigList)->ArgList(kSizes);

// Benchmarks random, uniformely distributed writes.
template <typename Depot>
void BM_UniformRandomWrite(benchmark::State& state) {
  auto num_elements = state.range(0);
  DepotHandler<Depot, kBranchFactor, kHashBoxSize> wrapper;

  // Initialize the depot with the total number of elements.
  auto& depot = wrapper.GetDepot();
  InitDepot(depot, num_elements);

  int i = 0;
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dist(0, num_elements - 1);
  for (auto _ : state) {
    auto value = std::array<std::byte, 1>{static_cast<std::byte>(i)};
    auto res = depot.Set(dist(gen), value);
  }
}

BENCHMARK_ALL(BM_UniformRandomWrite, DepotConfigList)->ArgList(kSizes);

// Benchmarks sequential read of keys.
template <typename Depot>
void BM_ExponentialRandomWrite(benchmark::State& state) {
  auto num_elements = state.range(0);
  DepotHandler<Depot, kBranchFactor, kHashBoxSize> wrapper;

  // Initialize the depot with the total number of elements.
  auto& depot = wrapper.GetDepot();
  InitDepot(depot, num_elements);

  int i = 0;
  std::random_device rd;
  std::mt19937 gen(rd());
  std::exponential_distribution<> dist(double(10) / num_elements);
  for (auto _ : state) {
    auto value = std::array<std::byte, 1>{static_cast<std::byte>(i)};
    auto res = depot.Set(dist(gen), value);
  }
}

BENCHMARK_ALL(BM_ExponentialRandomWrite, DepotConfigList)->ArgList(kSizes);

template <typename Depot, bool include_write_time>
void RunHashSequentialUpdates(benchmark::State& state) {
  auto num_elements = state.range(0);
  DepotHandler<Depot, kBranchFactor, kHashBoxSize> wrapper;

  // Initialize the depot with the total number of elements.
  auto& depot = wrapper.GetDepot();
  InitDepot(depot, num_elements);

  int i = 0;
  for (auto _ : state) {
    // Update a set of values.
    if (!include_write_time) state.PauseTiming();
    for (int j = 0; j < 100; j++) {
      auto value = std::array<std::byte, 4>{
          static_cast<std::byte>(i >> 24), static_cast<std::byte>(i >> 16),
          static_cast<std::byte>(i >> 8), static_cast<std::byte>(i)};
      auto res = depot.Set(i++ % num_elements, value);
    }
    if (!include_write_time) state.ResumeTiming();

    auto hash = depot.GetHash();
    benchmark::DoNotOptimize(hash);
  }
}

template <typename Depot>
void BM_HashSequentialUpdates(benchmark::State& state) {
  RunHashSequentialUpdates<Depot, false>(state);
}

BENCHMARK_ALL(BM_HashSequentialUpdates, DepotConfigList)->ArgList(kSizes);

template <typename Depot, bool include_write_time>
void RunHashUniformUpdates(benchmark::State& state) {
  auto num_elements = state.range(0);
  DepotHandler<Depot, kBranchFactor, kHashBoxSize> wrapper;

  // Initialize the depot with the total number of elements.
  auto& depot = wrapper.GetDepot();
  InitDepot(depot, num_elements);

  int i = 0;
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dist(0, num_elements - 1);
  for (auto _ : state) {
    // Update a set of values.
    if (!include_write_time) state.PauseTiming();
    for (int j = 0; j < 100; j++) {
      auto value = std::array<std::byte, 4>{
          static_cast<std::byte>(i >> 24), static_cast<std::byte>(i >> 16),
          static_cast<std::byte>(i >> 8), static_cast<std::byte>(i)};
      i++;
      auto res = depot.Set(dist(gen), value);
    }
    if (!include_write_time) state.ResumeTiming();

    auto hash = depot.GetHash();
    benchmark::DoNotOptimize(hash);
  }
}

template <typename Depot>
void BM_HashUniformUpdates(benchmark::State& state) {
  RunHashUniformUpdates<Depot, false>(state);
}

BENCHMARK_ALL(BM_HashUniformUpdates, DepotConfigList)->ArgList(kSizes);

template <typename Depot, bool include_write_time>
void RunHashExponentialUpdates(benchmark::State& state) {
  auto num_elements = state.range(0);
  DepotHandler<Depot, kBranchFactor, kHashBoxSize> wrapper;

  // Initialize the depot with the total number of elements.
  auto& depot = wrapper.GetDepot();
  InitDepot(depot, num_elements);

  int i = 0;
  std::random_device rd;
  std::mt19937 gen(rd());
  std::exponential_distribution<> dist(double(10) / num_elements);
  for (auto _ : state) {
    // Update a set of values.
    if (!include_write_time) state.PauseTiming();
    for (int j = 0; j < 100; j++) {
      auto value = std::array<std::byte, 4>{
          static_cast<std::byte>(i >> 24), static_cast<std::byte>(i >> 16),
          static_cast<std::byte>(i >> 8), static_cast<std::byte>(i)};
      i++;
      auto res = depot.Set(std::size_t(dist(gen)) % num_elements, value);
    }
    if (!include_write_time) state.ResumeTiming();

    auto hash = depot.GetHash();
    benchmark::DoNotOptimize(hash);
  }
}

template <typename Depot>
void BM_HashExponentialUpdates(benchmark::State& state) {
  RunHashExponentialUpdates<Depot, false>(state);
}

BENCHMARK_ALL(BM_HashExponentialUpdates, DepotConfigList)->ArgList(kSizes);

template <typename Depot>
void BM_SequentialWriteAndHash(benchmark::State& state) {
  RunHashSequentialUpdates<Depot, true>(state);
}

BENCHMARK_ALL(BM_SequentialWriteAndHash, DepotConfigList)->ArgList(kSizes);

template <typename Depot>
void BM_UniformWriteAndHash(benchmark::State& state) {
  RunHashUniformUpdates<Depot, true>(state);
}

BENCHMARK_ALL(BM_UniformWriteAndHash, DepotConfigList)->ArgList(kSizes);

template <typename Depot>
void BM_ExponentialWriteAndHash(benchmark::State& state) {
  RunHashExponentialUpdates<Depot, true>(state);
}

BENCHMARK_ALL(BM_ExponentialWriteAndHash, DepotConfigList)->ArgList(kSizes);

}  // namespace
}  // namespace carmen::backend::depot
