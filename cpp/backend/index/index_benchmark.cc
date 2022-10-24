#include <random>

#include "backend/common/file.h"
#include "backend/index/cache/cache.h"
#include "backend/index/file/index.h"
#include "backend/index/index_handler.h"
#include "backend/index/leveldb/single_file/test_util.h"
#include "backend/index/memory/index.h"
#include "backend/index/memory/linear_hash_index.h"
#include "benchmark/benchmark.h"

namespace carmen::backend::index {
namespace {

constexpr const std::size_t kPageSize = 1 << 12;  // = 4 KiB

using InMemoryIndex = InMemoryIndex<Key, std::uint32_t>;
using CachedInMemoryIndex = Cached<InMemoryIndex>;
using InMemoryLinearHashIndex = InMemoryLinearHashIndex<Key, std::uint32_t>;
using FileIndexInMemory =
    FileIndex<Key, std::uint32_t, InMemoryFile, kPageSize>;
using FileIndexOnDisk = FileIndex<Key, std::uint32_t, SingleFile, kPageSize>;
using CachedFileIndexOnDisk = Cached<FileIndexOnDisk>;
using KeySpacedLevelDBIndex = LevelDBKeySpaceTestAdapter<Key, std::uint32_t>;
using CachedKeySpacedLevelDBIndex = Cached<KeySpacedLevelDBIndex>;
using LevelDBIndex = LevelDBIndexTestAdapter<Key, std::uint32_t>;
using CachedLevelDBIndex = Cached<LevelDBIndex>;

// To run benchmarks, use the following command:
//    bazel run -c opt //backend/index:index_benchmark

Key ToKey(std::int64_t value) {
  return Key{static_cast<std::uint8_t>(value >> 32),
             static_cast<std::uint8_t>(value >> 24),
             static_cast<std::uint8_t>(value >> 16),
             static_cast<std::uint8_t>(value >> 8),
             static_cast<std::uint8_t>(value >> 0)};
}

// Benchmarks the sequential insertion of keys into indexes.
template <typename IndexHandler>
void BM_Insert(benchmark::State& state) {
  auto pre_loaded_num_elements = state.range(0);
  IndexHandler handler;
  auto& index = handler.GetIndex();

  // Fill in initial elements.
  for (std::int64_t i = 0; i < pre_loaded_num_elements; i++) {
    index.GetOrAdd(ToKey(i));
  }

  auto i = pre_loaded_num_elements;
  for (auto _ : state) {
    auto id = index.GetOrAdd(ToKey(i++));
    benchmark::DoNotOptimize(id);
  }
}

BENCHMARK(BM_Insert<IndexHandler<InMemoryIndex>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);  // 1<<30 skipped since this would require 36 GiB of memory

BENCHMARK(BM_Insert<IndexHandler<CachedInMemoryIndex>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);  // 1<<30 skipped since this would require 36 GiB of memory

BENCHMARK(BM_Insert<IndexHandler<InMemoryLinearHashIndex>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);  // 1<<30 skipped since this would require 36 GiB of memory

BENCHMARK(BM_Insert<IndexHandler<FileIndexInMemory>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);  // 1<<30 skipped since this would require 36 GiB of memory

BENCHMARK(BM_Insert<IndexHandler<FileIndexOnDisk>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24)
    ->Arg(1 << 30);

BENCHMARK(BM_Insert<IndexHandler<CachedFileIndexOnDisk>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24)
    ->Arg(1 << 30);

BENCHMARK(BM_Insert<IndexHandler<KeySpacedLevelDBIndex>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);

BENCHMARK(BM_Insert<IndexHandler<CachedKeySpacedLevelDBIndex>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);

BENCHMARK(BM_Insert<IndexHandler<LevelDBIndex>>)->Arg(1 << 20)->Arg(1 << 24);

BENCHMARK(BM_Insert<IndexHandler<CachedLevelDBIndex>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);

template <typename IndexHandler>
void BM_SequentialRead(benchmark::State& state) {
  auto pre_loaded_num_elements = state.range(0);
  IndexHandler handler;
  auto& index = handler.GetIndex();

  // Fill in initial elements.
  for (std::int64_t i = 0; i < pre_loaded_num_elements; i++) {
    index.GetOrAdd(ToKey(i));
  }

  auto i = 0;
  for (auto _ : state) {
    auto id = index.Get(ToKey(i++));
    benchmark::DoNotOptimize(id);
  }
}

BENCHMARK(BM_SequentialRead<IndexHandler<InMemoryIndex>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);  // 1<<30 skipped since this would require 36 GiB of memory

BENCHMARK(BM_SequentialRead<IndexHandler<CachedInMemoryIndex>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);  // 1<<30 skipped since this would require 36 GiB of memory

BENCHMARK(BM_SequentialRead<IndexHandler<InMemoryLinearHashIndex>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);  // 1<<30 skipped since this would require 36 GiB of memory

BENCHMARK(BM_SequentialRead<IndexHandler<FileIndexInMemory>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);  // 1<<30 skipped since this would require 36 GiB of memory

BENCHMARK(BM_SequentialRead<IndexHandler<FileIndexOnDisk>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24)
    ->Arg(1 << 30);

BENCHMARK(BM_SequentialRead<IndexHandler<CachedFileIndexOnDisk>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24)
    ->Arg(1 << 30);

BENCHMARK(BM_SequentialRead<IndexHandler<KeySpacedLevelDBIndex>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);

BENCHMARK(BM_SequentialRead<IndexHandler<CachedKeySpacedLevelDBIndex>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);

BENCHMARK(BM_SequentialRead<IndexHandler<LevelDBIndex>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);

BENCHMARK(BM_SequentialRead<IndexHandler<CachedLevelDBIndex>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);

template <typename IndexHandler>
void BM_UniformRandomRead(benchmark::State& state) {
  auto pre_loaded_num_elements = state.range(0);
  IndexHandler handler;
  auto& index = handler.GetIndex();

  // Fill in initial elements.
  for (std::int64_t i = 0; i < pre_loaded_num_elements; i++) {
    index.GetOrAdd(ToKey(i));
  }

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dist(0, pre_loaded_num_elements - 1);
  for (auto _ : state) {
    auto id = index.Get(ToKey(dist(gen)));
    benchmark::DoNotOptimize(id);
  }
}

BENCHMARK(BM_UniformRandomRead<IndexHandler<InMemoryIndex>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);  // 1<<30 skipped since this would require 36 GiB of memory

BENCHMARK(BM_UniformRandomRead<IndexHandler<CachedInMemoryIndex>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);  // 1<<30 skipped since this would require 36 GiB of memory

BENCHMARK(BM_UniformRandomRead<IndexHandler<InMemoryLinearHashIndex>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);  // 1<<30 skipped since this would require 36 GiB of memory

BENCHMARK(BM_UniformRandomRead<IndexHandler<FileIndexInMemory>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);  // 1<<30 skipped since this would require 36 GiB of memory

BENCHMARK(BM_UniformRandomRead<IndexHandler<FileIndexOnDisk>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24)
    ->Arg(1 << 30);

BENCHMARK(BM_UniformRandomRead<IndexHandler<CachedFileIndexOnDisk>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24)
    ->Arg(1 << 30);

BENCHMARK(BM_UniformRandomRead<IndexHandler<KeySpacedLevelDBIndex>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);

BENCHMARK(BM_UniformRandomRead<IndexHandler<CachedKeySpacedLevelDBIndex>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);

BENCHMARK(BM_UniformRandomRead<IndexHandler<LevelDBIndex>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);

BENCHMARK(BM_UniformRandomRead<IndexHandler<CachedLevelDBIndex>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);

template <typename IndexHandler>
void BM_ExponentialRandomRead(benchmark::State& state) {
  auto pre_loaded_num_elements = state.range(0);
  IndexHandler handler;
  auto& index = handler.GetIndex();

  // Fill in initial elements.
  for (std::int64_t i = 0; i < pre_loaded_num_elements; i++) {
    index.GetOrAdd(ToKey(i));
  }

  std::random_device rd;
  std::mt19937 gen(rd());
  std::exponential_distribution<> dist(double(10) / pre_loaded_num_elements);
  for (auto _ : state) {
    auto id = index.Get(ToKey(dist(gen)));
    benchmark::DoNotOptimize(id);
  }
}

BENCHMARK(BM_ExponentialRandomRead<IndexHandler<InMemoryIndex>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);  // 1<<30 skipped since this would require 36 GiB of memory

BENCHMARK(BM_ExponentialRandomRead<IndexHandler<CachedInMemoryIndex>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);  // 1<<30 skipped since this would require 36 GiB of memory

BENCHMARK(BM_ExponentialRandomRead<IndexHandler<InMemoryLinearHashIndex>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);  // 1<<30 skipped since this would require 36 GiB of memory

BENCHMARK(BM_ExponentialRandomRead<IndexHandler<FileIndexInMemory>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);  // 1<<30 skipped since this would require 36 GiB of memory

BENCHMARK(BM_ExponentialRandomRead<IndexHandler<FileIndexOnDisk>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24)
    ->Arg(1 << 30);

BENCHMARK(BM_ExponentialRandomRead<IndexHandler<CachedFileIndexOnDisk>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24)
    ->Arg(1 << 30);

BENCHMARK(BM_ExponentialRandomRead<IndexHandler<KeySpacedLevelDBIndex>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);

BENCHMARK(BM_ExponentialRandomRead<IndexHandler<CachedKeySpacedLevelDBIndex>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);

BENCHMARK(BM_ExponentialRandomRead<IndexHandler<LevelDBIndex>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);

BENCHMARK(BM_ExponentialRandomRead<IndexHandler<CachedLevelDBIndex>>)
    ->Arg(1 << 20)
    ->Arg(1 << 24);

template <typename IndexHandler>
void BM_Hash(benchmark::State& state) {
  auto pre_loaded_num_elements = state.range(0);

  // A new index is created each time since otherwise it quickly fills up all
  // of the main memory.
  IndexHandler handler;
  auto& index = handler.GetIndex();

  // Fill in initial elements.
  for (std::int64_t i = 0; i < pre_loaded_num_elements; i++) {
    index.GetOrAdd(ToKey(i));
  }
  index.GetHash();
  auto i = pre_loaded_num_elements;

  for (auto _ : state) {
    state.PauseTiming();
    for (int j = 0; j < 100; j++) {
      index.GetOrAdd(ToKey(i++));
    }
    state.ResumeTiming();
    auto hash = index.GetHash();
    benchmark::DoNotOptimize(hash);
  }
}

BENCHMARK(BM_Hash<IndexHandler<InMemoryIndex>>)
    ->Arg(1 << 10)
    ->Arg(1 << 14);  // skipped larger cases since it takes forever to hash
                     // initial entries

BENCHMARK(BM_Hash<IndexHandler<CachedInMemoryIndex>>)
    ->Arg(1 << 10)
    ->Arg(1 << 14);  // skipped larger cases since it takes forever to hash
                     // initial entries

BENCHMARK(BM_Hash<IndexHandler<InMemoryLinearHashIndex>>)
    ->Arg(1 << 10)
    ->Arg(1 << 14);  // skipped larger cases since it takes forever to hash
                     // initial entries

BENCHMARK(BM_Hash<IndexHandler<FileIndexInMemory>>)
    ->Arg(1 << 10)
    ->Arg(1 << 14);  // skipped larger cases since it takes forever to hash
                     // initial entries

BENCHMARK(BM_Hash<IndexHandler<FileIndexOnDisk>>)
    ->Arg(1 << 10)
    ->Arg(1 << 14);  // skipped larger cases since it takes forever to hash
                     // initial entries

BENCHMARK(BM_Hash<IndexHandler<CachedFileIndexOnDisk>>)
    ->Arg(1 << 10)
    ->Arg(1 << 14);  // skipped larger cases since it takes forever to hash
                     // initial entries

BENCHMARK(BM_Hash<IndexHandler<KeySpacedLevelDBIndex>>)
    ->Arg(1 << 10)
    ->Arg(1 << 14);  // skipped larger cases since it takes forever to hash
                     // initial entries

BENCHMARK(BM_Hash<IndexHandler<CachedKeySpacedLevelDBIndex>>)
    ->Arg(1 << 10)
    ->Arg(1 << 14);  // skipped larger cases since it takes forever to hash
                     // initial entries

BENCHMARK(BM_Hash<IndexHandler<LevelDBIndex>>)
    ->Arg(1 << 10)
    ->Arg(1 << 14);  // skipped larger cases since it takes forever to hash
                     // initial entries

BENCHMARK(BM_Hash<IndexHandler<CachedLevelDBIndex>>)
    ->Arg(1 << 10)
    ->Arg(1 << 14);  // skipped larger cases since it takes forever to hash
                     // initial entries
}  // namespace
}  // namespace carmen::backend::index
