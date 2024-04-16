/*
 * Copyright (c) 2024 Fantom Foundation
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE file and at fantom.foundation/bsl11.
 *
 * Change Date: 2028-4-16
 *
 * On the date above, in accordance with the Business Source License, use of
 * this software will be governed by the GNU Lesser General Public License v3.
 */

#include <random>

#include "backend/store/leveldb/store.h"
#include "backend/store/store_handler.h"
#include "benchmark/benchmark.h"
#include "common/benchmark.h"
#include "common/file_util.h"
#include "common/status_test_util.h"

namespace carmen::backend::store {
namespace {

constexpr const std::size_t kPageSize = 1 << 12;  // = 4 KiB
constexpr const std::size_t kBranchFactor = 32;

// To run benchmarks, use the following command:
//    bazel run -c opt //backend/store:store_benchmark

// Defines the list of configurations to be benchmarked.
BENCHMARK_TYPE_LIST(StoreConfigList, (ReferenceStore<kPageSize>),
                    (InMemoryStore<int, Value, kPageSize>),
                    (LevelDbStore<int, Value, kPageSize>),
                    (EagerFileStore<int, Value, InMemoryFile, kPageSize>),
                    (EagerFileStore<int, Value, SingleFile, kPageSize>),
                    (LazyFileStore<int, Value, SingleFile, kPageSize>));

// Defines the list of problem sizes.
const auto kSizes = std::vector<int64_t>({1 << 20, 1 << 24});

// Utility to initialize a store with a given number of elements.
template <typename Store>
void InitStore(Store& store, std::size_t num_elements) {
  for (std::size_t i = 0; i < num_elements; i++) {
    ASSERT_OK(store.Set(i, Value{1, 2, 3, 4}));
  }
  ASSERT_OK(store.GetHash());
}

// Benchmarks the sequential insertion of keys into stores.
template <typename Store>
void BM_SequentialInsert(benchmark::State& state) {
  TempDir dir;
  auto num_elements = state.range(0);
  for (auto _ : state) {
    Context ctx;
    ASSERT_OK_AND_ASSIGN(auto store, Store::Open(ctx, dir, kBranchFactor));
    for (int i = 0; i < num_elements; i++) {
      ASSERT_OK(store.Set(i, Value{}));
    }
  }
}

BENCHMARK_ALL(BM_SequentialInsert, StoreConfigList)->ArgList(kSizes);

// Benchmarks the appending of new elements to the store.
template <typename Store>
void BM_Insert(benchmark::State& state) {
  // The size of the store before the inserts.
  auto num_elements = state.range(0);

  // Initialize the store with the initial number of elements.
  TempDir dir;
  Context ctx;
  ASSERT_OK_AND_ASSIGN(auto store, Store::Open(ctx, dir, kBranchFactor));
  InitStore(store, num_elements);

  // Append additional elements to the end of the store.
  auto i = num_elements;
  for (auto _ : state) {
    ASSERT_OK(store.Set(i++, Value{}));
  }
}

BENCHMARK_ALL(BM_Insert, StoreConfigList)->ArgList(kSizes);

// Benchmarks sequential read of keys.
template <typename Store>
void BM_SequentialRead(benchmark::State& state) {
  auto num_elements = state.range(0);

  // Initialize the store with the total number of elements.
  TempDir dir;
  Context ctx;
  ASSERT_OK_AND_ASSIGN(auto store, Store::Open(ctx, dir, kBranchFactor));
  InitStore(store, num_elements);

  int i = 0;
  for (auto _ : state) {
    auto value = store.Get(i++ % num_elements);
    benchmark::DoNotOptimize(value);
  }
}

BENCHMARK_ALL(BM_SequentialRead, StoreConfigList)->ArgList(kSizes);

// Benchmarks random, uniformly distributed reads
template <typename Store>
void BM_UniformRandomRead(benchmark::State& state) {
  auto num_elements = state.range(0);

  // Initialize the store with the total number of elements.
  TempDir dir;
  Context ctx;
  ASSERT_OK_AND_ASSIGN(auto store, Store::Open(ctx, dir, kBranchFactor));
  InitStore(store, num_elements);

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dist(0, num_elements - 1);
  for (auto _ : state) {
    auto value = store.Get(dist(gen));
    benchmark::DoNotOptimize(value);
  }
}

BENCHMARK_ALL(BM_UniformRandomRead, StoreConfigList)->ArgList(kSizes);

// Benchmarks random, exponentially distributed reads
template <typename Store>
void BM_ExponentialRandomRead(benchmark::State& state) {
  auto num_elements = state.range(0);

  // Initialize the store with the total number of elements.
  TempDir dir;
  Context ctx;
  ASSERT_OK_AND_ASSIGN(auto store, Store::Open(ctx, dir, kBranchFactor));
  InitStore(store, num_elements);

  std::random_device rd;
  std::mt19937 gen(rd());
  std::exponential_distribution<> dist(double(10) / num_elements);
  for (auto _ : state) {
    auto value = store.Get(static_cast<std::size_t>(dist(gen)) % num_elements);
    benchmark::DoNotOptimize(value);
  }
}

BENCHMARK_ALL(BM_ExponentialRandomRead, StoreConfigList)->ArgList(kSizes);

// Benchmarks sequential writes of keys.
template <typename Store>
void BM_SequentialWrite(benchmark::State& state) {
  auto num_elements = state.range(0);

  // Initialize the store with the total number of elements.
  TempDir dir;
  Context ctx;
  ASSERT_OK_AND_ASSIGN(auto store, Store::Open(ctx, dir, kBranchFactor));
  InitStore(store, num_elements);

  int i = 0;
  for (auto _ : state) {
    Value value{static_cast<std::uint8_t>(i)};
    ASSERT_OK(store.Set(i++ % num_elements, value));
  }
}

BENCHMARK_ALL(BM_SequentialWrite, StoreConfigList)->ArgList(kSizes);

// Benchmarks random, uniformly distributed writes.
template <typename Store>
void BM_UniformRandomWrite(benchmark::State& state) {
  auto num_elements = state.range(0);

  // Initialize the store with the total number of elements.
  TempDir dir;
  Context ctx;
  ASSERT_OK_AND_ASSIGN(auto store, Store::Open(ctx, dir, kBranchFactor));
  InitStore(store, num_elements);

  int i = 0;
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dist(0, num_elements - 1);
  for (auto _ : state) {
    Value value{static_cast<std::uint8_t>(i++)};
    ASSERT_OK(store.Set(dist(gen), value));
  }
}

BENCHMARK_ALL(BM_UniformRandomWrite, StoreConfigList)->ArgList(kSizes);

// Benchmarks sequential read of keys.
template <typename Store>
void BM_ExponentialRandomWrite(benchmark::State& state) {
  auto num_elements = state.range(0);

  // Initialize the store with the total number of elements.
  TempDir dir;
  Context ctx;
  ASSERT_OK_AND_ASSIGN(auto store, Store::Open(ctx, dir, kBranchFactor));
  InitStore(store, num_elements);

  int i = 0;
  std::random_device rd;
  std::mt19937 gen(rd());
  std::exponential_distribution<> dist(double(10) / num_elements);
  for (auto _ : state) {
    Value value{static_cast<std::uint8_t>(i++)};
    ASSERT_OK(store.Set(dist(gen), value));
  }
}

BENCHMARK_ALL(BM_ExponentialRandomWrite, StoreConfigList)->ArgList(kSizes);

template <typename Store, bool include_write_time>
void RunHashSequentialUpdates(benchmark::State& state) {
  auto num_elements = state.range(0);

  // Initialize the store with the total number of elements.
  TempDir dir;
  Context ctx;
  ASSERT_OK_AND_ASSIGN(auto store, Store::Open(ctx, dir, kBranchFactor));
  InitStore(store, num_elements);
  ASSERT_OK(store.GetHash());

  int i = 0;
  for (auto _ : state) {
    // Update a set of values.
    if (!include_write_time) state.PauseTiming();
    for (int j = 0; j < 100; j++) {
      Value value{static_cast<std::uint8_t>(i >> 24),
                  static_cast<std::uint8_t>(i >> 16),
                  static_cast<std::uint8_t>(i >> 8),
                  static_cast<std::uint8_t>(i)};
      ASSERT_OK(store.Set(i++ % num_elements, value));
    }
    if (!include_write_time) state.ResumeTiming();

    auto hash = store.GetHash();
    benchmark::DoNotOptimize(hash);
  }
}

template <typename Store>
void BM_HashSequentialUpdates(benchmark::State& state) {
  RunHashSequentialUpdates<Store, false>(state);
}

BENCHMARK_ALL(BM_HashSequentialUpdates, StoreConfigList)->ArgList(kSizes);

template <typename Store, bool include_write_time>
void RunHashUniformUpdates(benchmark::State& state) {
  auto num_elements = state.range(0);

  // Initialize the store with the total number of elements.
  TempDir dir;
  Context ctx;
  ASSERT_OK_AND_ASSIGN(auto store, Store::Open(ctx, dir, kBranchFactor));
  InitStore(store, num_elements);
  ASSERT_OK(store.GetHash());

  int i = 0;
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dist(0, num_elements - 1);
  for (auto _ : state) {
    // Update a set of values.
    if (!include_write_time) state.PauseTiming();
    for (int j = 0; j < 100; j++) {
      Value value{static_cast<std::uint8_t>(i >> 24),
                  static_cast<std::uint8_t>(i >> 16),
                  static_cast<std::uint8_t>(i >> 8),
                  static_cast<std::uint8_t>(i)};
      i++;
      ASSERT_OK(store.Set(dist(gen), value));
    }
    if (!include_write_time) state.ResumeTiming();

    auto hash = store.GetHash();
    benchmark::DoNotOptimize(hash);
  }
}

template <typename Store>
void BM_HashUniformUpdates(benchmark::State& state) {
  RunHashUniformUpdates<Store, false>(state);
}

BENCHMARK_ALL(BM_HashUniformUpdates, StoreConfigList)->ArgList(kSizes);

template <typename Store, bool include_write_time>
void RunHashExponentialUpdates(benchmark::State& state) {
  auto num_elements = state.range(0);

  // Initialize the store with the total number of elements.
  TempDir dir;
  Context ctx;
  ASSERT_OK_AND_ASSIGN(auto store, Store::Open(ctx, dir, kBranchFactor));
  InitStore(store, num_elements);
  ASSERT_OK(store.GetHash());

  int i = 0;
  std::random_device rd;
  std::mt19937 gen(rd());
  std::exponential_distribution<> dist(double(10) / num_elements);
  for (auto _ : state) {
    // Update a set of values.
    if (!include_write_time) state.PauseTiming();
    for (int j = 0; j < 100; j++) {
      Value value{static_cast<std::uint8_t>(i >> 24),
                  static_cast<std::uint8_t>(i >> 16),
                  static_cast<std::uint8_t>(i >> 8),
                  static_cast<std::uint8_t>(i)};
      i++;
      ASSERT_OK(store.Set(std::size_t(dist(gen)) % num_elements, value));
    }
    if (!include_write_time) state.ResumeTiming();

    auto hash = store.GetHash();
    benchmark::DoNotOptimize(hash);
  }
}

template <typename Store>
void BM_HashExponentialUpdates(benchmark::State& state) {
  RunHashExponentialUpdates<Store, false>(state);
}

BENCHMARK_ALL(BM_HashExponentialUpdates, StoreConfigList)->ArgList(kSizes);

template <typename Store>
void BM_SequentialWriteAndHash(benchmark::State& state) {
  RunHashSequentialUpdates<Store, true>(state);
}

BENCHMARK_ALL(BM_SequentialWriteAndHash, StoreConfigList)->ArgList(kSizes);

template <typename Store>
void BM_UniformWriteAndHash(benchmark::State& state) {
  RunHashUniformUpdates<Store, true>(state);
}

BENCHMARK_ALL(BM_UniformWriteAndHash, StoreConfigList)->ArgList(kSizes);

template <typename Store>
void BM_ExponentialWriteAndHash(benchmark::State& state) {
  RunHashExponentialUpdates<Store, true>(state);
}

BENCHMARK_ALL(BM_ExponentialWriteAndHash, StoreConfigList)->ArgList(kSizes);

}  // namespace
}  // namespace carmen::backend::store
