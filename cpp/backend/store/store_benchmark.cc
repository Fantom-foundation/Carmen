#include <random>

#include "backend/store/store_handler.h"
#include "benchmark/benchmark.h"

namespace carmen::backend::store {
namespace {

constexpr const std::size_t kPageSize = 1 << 12;  // = 4 KiB
constexpr const std::size_t kBranchFactor = 32;

// To run benchmarks, use the following command:
//    bazel run -c opt //backend/store:store_benchmark

// Benchmarks the sequential insertion of keys into stores.
template <typename StoreHandler>
void BM_SequentialInsert(benchmark::State& state) {
  auto num_elements = state.range(0);
  for (auto _ : state) {
    StoreHandler wrapper;
    auto& store = wrapper.GetStore();
    for (int i = 0; i < num_elements; i++) {
      store.Set(i, Value{});
    }
  }
}

BENCHMARK(
    BM_SequentialInsert<StoreHandler<ReferenceStore<kPageSize>, kBranchFactor>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);  // 1<<30 skipped since this would require 32 GiB of memory

BENCHMARK(BM_SequentialInsert<StoreHandler<
              FileStore<int, Value, InMemoryFile, kPageSize>, kBranchFactor>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);  // 1<<30 skipped since this would require 32 GiB of memory

BENCHMARK(BM_SequentialInsert<StoreHandler<
              FileStore<int, Value, SingleFile, kPageSize>, kBranchFactor>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);  // 1<<30 skipped since it takes too long to run

// Benchmarks the appending of new elements to the store.
template <typename StoreHandler>
void BM_Insert(benchmark::State& state) {
  // The size of the store before the inserts.
  auto num_elements = state.range(0);

  // Initialize the store with the initial number of elements.
  StoreHandler wrapper;
  auto& store = wrapper.GetStore();
  store.Get(num_elements - 1);

  // Append additional elements to the end of the store.
  auto i = num_elements;
  for (auto _ : state) {
    store.Set(i++, Value{});
  }
}

BENCHMARK(BM_Insert<StoreHandler<ReferenceStore<kPageSize>, kBranchFactor>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);  // 1<<30 skipped since this would require 32 GiB of memory

BENCHMARK(BM_Insert<StoreHandler<FileStore<int, Value, InMemoryFile, kPageSize>,
                                 kBranchFactor>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);  // 1<<30 skipped since this would require 32 GiB of memory

BENCHMARK(BM_Insert<StoreHandler<FileStore<int, Value, SingleFile, kPageSize>,
                                 kBranchFactor>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);  // 1<<30 skipped since it takes too long to run

// Benchmarks sequential read of read of keys.
template <typename StoreHandler>
void BM_SequentialRead(benchmark::State& state) {
  auto num_elements = state.range(0);
  StoreHandler wrapper;

  // Initialize the store with the total number of elements.
  auto& store = wrapper.GetStore();
  store.Get(num_elements - 1);

  int i = 0;
  for (auto _ : state) {
    auto value = store.Get(i++ % num_elements);
    benchmark::DoNotOptimize(value);
  }
}

BENCHMARK(
    BM_SequentialRead<StoreHandler<ReferenceStore<kPageSize>, kBranchFactor>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);  // 1<<30 skipped since this would require 32 GiB of memory

BENCHMARK(BM_SequentialRead<StoreHandler<
              FileStore<int, Value, InMemoryFile, kPageSize>, kBranchFactor>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);  // 1<<30 skipped since this would require 32 GiB of memory

BENCHMARK(BM_SequentialRead<StoreHandler<
              FileStore<int, Value, SingleFile, kPageSize>, kBranchFactor>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);  // 1<<30 skipped since it takes too long to run

// Benchmarks random, uniformely distributed reads
template <typename StoreHandler>
void BM_UniformRandomRead(benchmark::State& state) {
  auto num_elements = state.range(0);
  StoreHandler wrapper;

  // Initialize the store with the total number of elements.
  auto& store = wrapper.GetStore();
  store.Get(num_elements - 1);

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dist(0, num_elements - 1);
  for (auto _ : state) {
    auto value = store.Get(dist(gen));
    benchmark::DoNotOptimize(value);
  }
}

BENCHMARK(BM_UniformRandomRead<
              StoreHandler<ReferenceStore<kPageSize>, kBranchFactor>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);  // 1<<30 skipped since this would require 32 GiB of memory

BENCHMARK(BM_UniformRandomRead<StoreHandler<
              FileStore<int, Value, InMemoryFile, kPageSize>, kBranchFactor>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);  // 1<<30 skipped since this would require 32 GiB of memory

BENCHMARK(BM_UniformRandomRead<StoreHandler<
              FileStore<int, Value, SingleFile, kPageSize>, kBranchFactor>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);  // 1<<30 skipped since it takes too long to run

// Benchmarks random, exponentially distributed reads
template <typename StoreHandler>
void BM_ExponentialRandomRead(benchmark::State& state) {
  auto num_elements = state.range(0);
  StoreHandler wrapper;

  // Initialize the store with the total number of elements.
  auto& store = wrapper.GetStore();
  store.Get(num_elements - 1);

  std::random_device rd;
  std::mt19937 gen(rd());
  std::exponential_distribution<> dist(double(10) / num_elements);
  for (auto _ : state) {
    auto value = store.Get(static_cast<std::size_t>(dist(gen)) % num_elements);
    benchmark::DoNotOptimize(value);
  }
}

BENCHMARK(BM_ExponentialRandomRead<
              StoreHandler<ReferenceStore<kPageSize>, kBranchFactor>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);  // 1<<30 skipped since this would require 32 GiB of memory

BENCHMARK(BM_ExponentialRandomRead<StoreHandler<
              FileStore<int, Value, InMemoryFile, kPageSize>, kBranchFactor>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);  // 1<<30 skipped since this would require 32 GiB of memory

BENCHMARK(BM_ExponentialRandomRead<StoreHandler<
              FileStore<int, Value, SingleFile, kPageSize>, kBranchFactor>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);  // 1<<30 skipped since it takes too long to run

// Benchmarks sequential writes of keys.
template <typename StoreHandler>
void BM_SequentialWrite(benchmark::State& state) {
  auto num_elements = state.range(0);
  StoreHandler wrapper;

  // Initialize the store with the total number of elements.
  auto& store = wrapper.GetStore();
  store.Get(num_elements - 1);

  int i = 0;
  for (auto _ : state) {
    Value value{static_cast<std::uint8_t>(i)};
    store.Set(i++ % num_elements, value);
  }
}

BENCHMARK(
    BM_SequentialWrite<StoreHandler<ReferenceStore<kPageSize>, kBranchFactor>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);  // 1<<30 skipped since this would require 32 GiB of memory

BENCHMARK(BM_SequentialWrite<StoreHandler<
              FileStore<int, Value, InMemoryFile, kPageSize>, kBranchFactor>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);  // 1<<30 skipped since this would require 32 GiB of memory

BENCHMARK(BM_SequentialWrite<StoreHandler<
              FileStore<int, Value, SingleFile, kPageSize>, kBranchFactor>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);  // 1<<30 skipped since it takes too long to run

// Benchmarks random, uniformely distributed writes.
template <typename StoreHandler>
void BM_UniformRandomWrite(benchmark::State& state) {
  auto num_elements = state.range(0);
  StoreHandler wrapper;

  // Initialize the store with the total number of elements.
  auto& store = wrapper.GetStore();
  store.Get(num_elements - 1);

  int i = 0;
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dist(0, num_elements - 1);
  for (auto _ : state) {
    Value value{static_cast<std::uint8_t>(i++)};
    store.Set(dist(gen), value);
  }
}

BENCHMARK(BM_UniformRandomWrite<
              StoreHandler<ReferenceStore<kPageSize>, kBranchFactor>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);  // 1<<30 skipped since this would require 32 GiB of memory

BENCHMARK(BM_UniformRandomWrite<StoreHandler<
              FileStore<int, Value, InMemoryFile, kPageSize>, kBranchFactor>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);  // 1<<30 skipped since this would require 32 GiB of memory

BENCHMARK(BM_UniformRandomWrite<StoreHandler<
              FileStore<int, Value, SingleFile, kPageSize>, kBranchFactor>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);  // 1<<30 skipped since it takes too long to run

// Benchmarks sequential read of read of keys.
template <typename StoreHandler>
void BM_ExponentialRandomWrite(benchmark::State& state) {
  auto num_elements = state.range(0);
  StoreHandler wrapper;

  // Initialize the store with the total number of elements.
  auto& store = wrapper.GetStore();
  store.Get(num_elements - 1);

  int i = 0;
  std::random_device rd;
  std::mt19937 gen(rd());
  std::exponential_distribution<> dist(double(10) / num_elements);
  for (auto _ : state) {
    Value value{static_cast<std::uint8_t>(i++)};
    store.Set(dist(gen), value);
  }
}

BENCHMARK(BM_ExponentialRandomWrite<
              StoreHandler<ReferenceStore<kPageSize>, kBranchFactor>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);  // 1<<30 skipped since this would require 32 GiB of memory

BENCHMARK(BM_ExponentialRandomWrite<StoreHandler<
              FileStore<int, Value, InMemoryFile, kPageSize>, kBranchFactor>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);  // 1<<30 skipped since this would require 32 GiB of memory

BENCHMARK(BM_ExponentialRandomWrite<StoreHandler<
              FileStore<int, Value, SingleFile, kPageSize>, kBranchFactor>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);  // 1<<30 skipped since it takes too long to run

template <typename StoreHandler, bool include_write_time>
void RunHashSequentialUpdates(benchmark::State& state) {
  auto num_elements = state.range(0);
  StoreHandler wrapper;

  // Initialize the store with the total number of elements.
  auto& store = wrapper.GetStore();
  store.Get(num_elements - 1);
  store.GetHash();

  int i = 0;
  for (auto _ : state) {
    // Update a set of values.
    if (!include_write_time) state.PauseTiming();
    for (int j = 0; j < 100; j++) {
      Value value{static_cast<std::uint8_t>(i >> 24),
                  static_cast<std::uint8_t>(i >> 16),
                  static_cast<std::uint8_t>(i >> 8),
                  static_cast<std::uint8_t>(i)};
      store.Set(i++ % num_elements, value);
    }
    if (!include_write_time) state.ResumeTiming();

    auto hash = store.GetHash();
    benchmark::DoNotOptimize(hash);
  }
}

template <typename StoreHandler>
void BM_HashSequentialUpdates(benchmark::State& state) {
  RunHashSequentialUpdates<StoreHandler, false>(state);
}

BENCHMARK(BM_HashSequentialUpdates<
              StoreHandler<ReferenceStore<kPageSize>, kBranchFactor>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);  // 1<<30 skipped since this would require 32 GiB of memory

BENCHMARK(BM_HashSequentialUpdates<StoreHandler<
              FileStore<int, Value, InMemoryFile, kPageSize>, kBranchFactor>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);  // 1<<30 skipped since this would require 32 GiB of memory

BENCHMARK(BM_HashSequentialUpdates<StoreHandler<
              FileStore<int, Value, SingleFile, kPageSize>, kBranchFactor>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);  // 1<<30 skipped since it takes too long to run

template <typename StoreHandler, bool include_write_time>
void RunHashUniformUpdates(benchmark::State& state) {
  auto num_elements = state.range(0);
  StoreHandler wrapper;

  // Initialize the store with the total number of elements.
  auto& store = wrapper.GetStore();
  store.Get(num_elements - 1);
  store.GetHash();

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
      store.Set(dist(gen), value);
    }
    if (!include_write_time) state.ResumeTiming();

    auto hash = store.GetHash();
    benchmark::DoNotOptimize(hash);
  }
}

template <typename StoreHandler>
void BM_HashUniformUpdates(benchmark::State& state) {
  RunHashUniformUpdates<StoreHandler, false>(state);
}

BENCHMARK(BM_HashUniformUpdates<
              StoreHandler<ReferenceStore<kPageSize>, kBranchFactor>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);  // 1<<30 skipped since this would require 32 GiB of memory

BENCHMARK(BM_HashUniformUpdates<StoreHandler<
              FileStore<int, Value, InMemoryFile, kPageSize>, kBranchFactor>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);  // 1<<30 skipped since this would require 32 GiB of memory

BENCHMARK(BM_HashUniformUpdates<StoreHandler<
              FileStore<int, Value, SingleFile, kPageSize>, kBranchFactor>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);  // 1<<30 skipped since it takes too long to run

template <typename StoreHandler, bool include_write_time>
void RunHashExponentialUpdates(benchmark::State& state) {
  auto num_elements = state.range(0);
  StoreHandler wrapper;

  // Initialize the store with the total number of elements.
  auto& store = wrapper.GetStore();
  store.Get(num_elements - 1);
  store.GetHash();

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
      store.Set(std::size_t(dist(gen)) % num_elements, value);
    }
    if (!include_write_time) state.ResumeTiming();

    auto hash = store.GetHash();
    benchmark::DoNotOptimize(hash);
  }
}

template <typename StoreHandler>
void BM_HashExponentialUpdates(benchmark::State& state) {
  RunHashExponentialUpdates<StoreHandler, false>(state);
}

BENCHMARK(BM_HashExponentialUpdates<
              StoreHandler<ReferenceStore<kPageSize>, kBranchFactor>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);  // 1<<30 skipped since this would require 32 GiB of memory

BENCHMARK(BM_HashExponentialUpdates<StoreHandler<
              FileStore<int, Value, InMemoryFile, kPageSize>, kBranchFactor>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);  // 1<<30 skipped since this would require 32 GiB of memory

BENCHMARK(BM_HashExponentialUpdates<StoreHandler<
              FileStore<int, Value, SingleFile, kPageSize>, kBranchFactor>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);  // 1<<30 skipped since it takes too long to run

template <typename StoreHandler>
void BM_SequentialWriteAndHash(benchmark::State& state) {
  RunHashSequentialUpdates<StoreHandler, true>(state);
}

BENCHMARK(BM_SequentialWriteAndHash<
              StoreHandler<ReferenceStore<kPageSize>, kBranchFactor>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);  // 1<<30 skipped since this would require 32 GiB of memory

BENCHMARK(BM_SequentialWriteAndHash<StoreHandler<
              FileStore<int, Value, InMemoryFile, kPageSize>, kBranchFactor>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);  // 1<<30 skipped since this would require 32 GiB of memory

BENCHMARK(BM_SequentialWriteAndHash<StoreHandler<
              FileStore<int, Value, SingleFile, kPageSize>, kBranchFactor>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);  // 1<<30 skipped since it takes too long to run

template <typename StoreHandler>
void BM_UniformWriteAndHash(benchmark::State& state) {
  RunHashUniformUpdates<StoreHandler, true>(state);
}

BENCHMARK(BM_UniformWriteAndHash<
              StoreHandler<ReferenceStore<kPageSize>, kBranchFactor>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);  // 1<<30 skipped since this would require 32 GiB of memory

BENCHMARK(BM_UniformWriteAndHash<StoreHandler<
              FileStore<int, Value, InMemoryFile, kPageSize>, kBranchFactor>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);  // 1<<30 skipped since this would require 32 GiB of memory

BENCHMARK(BM_UniformWriteAndHash<StoreHandler<
              FileStore<int, Value, SingleFile, kPageSize>, kBranchFactor>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);  // 1<<30 skipped since it takes too long to run

template <typename StoreHandler>
void BM_ExponentialWriteAndHash(benchmark::State& state) {
  RunHashExponentialUpdates<StoreHandler, true>(state);
}

BENCHMARK(BM_ExponentialWriteAndHash<
              StoreHandler<ReferenceStore<kPageSize>, kBranchFactor>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);  // 1<<30 skipped since this would require 32 GiB of memory

BENCHMARK(BM_ExponentialWriteAndHash<StoreHandler<
              FileStore<int, Value, InMemoryFile, kPageSize>, kBranchFactor>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);  // 1<<30 skipped since this would require 32 GiB of memory

BENCHMARK(BM_ExponentialWriteAndHash<StoreHandler<
              FileStore<int, Value, SingleFile, kPageSize>, kBranchFactor>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);  // 1<<30 skipped since it takes too long to run

}  // namespace
}  // namespace carmen::backend::store
