#include <concepts>
#include <random>

#include "backend/index/leveldb/multi_db/index.h"
#include "backend/index/leveldb/single_db/index.h"
#include "benchmark/benchmark.h"
#include "common/file_util.h"
#include "common/type.h"

namespace carmen::backend::index {
namespace {

// To run benchmarks, use the following command:
//    bazel run -c opt //backend/index/leveldb:index_benchmark

template <Trivial K, std::integral I>
class SingleIndexBM {
 public:
  explicit SingleIndexBM(std::uint8_t num_indexes) {
    assert(num_indexes > 0 && "num_indexes must be greater than 0");
    // initialize index leveldb index
    auto index = *SingleLevelDbIndex::Open(dir_.GetPath());
    for (std::uint8_t i = 0; i < num_indexes; ++i) {
      // create key space
      indexes_.push_back(index.template KeySpace<K, I>(i));
    }
  }
  LevelDbKeySpace<K, I>& GetIndex(std::uint8_t index) {
    return indexes_[index];
  }

 private:
  TempDir dir_;
  std::vector<LevelDbKeySpace<K, I>> indexes_;
};

template <Trivial K, std::integral I>
class MultiIndexBM {
 public:
  explicit MultiIndexBM(std::uint8_t num_indexes) {
    assert(num_indexes > 0 && "num_indexes must be greater than 0");
    for (std::uint8_t i = 0; i < num_indexes; ++i) {
      auto dir = TempDir();
      indexes_.push_back(*MultiLevelDbIndex<K, I>::Open(dir.GetPath()));
      dirs_.push_back(std::move(dir));
    }
  }
  MultiLevelDbIndex<K, I>& GetIndex(std::uint8_t index) {
    return indexes_[index];
  }

 private:
  std::vector<TempDir> dirs_;
  std::vector<MultiLevelDbIndex<K, I>> indexes_;
};

using SingleIndex = SingleIndexBM<Key, std::uint64_t>;
using MultiIndex = MultiIndexBM<Key, std::uint64_t>;

Key ToKey(std::int64_t value) {
  return Key{static_cast<std::uint8_t>(value >> 32),
             static_cast<std::uint8_t>(value >> 24),
             static_cast<std::uint8_t>(value >> 16),
             static_cast<std::uint8_t>(value >> 8),
             static_cast<std::uint8_t>(value >> 0)};
}

// Benchmarks the sequential insertion of keys into indexes.
template <typename LevelDbIndex>
void BM_Insert(benchmark::State& state) {
  auto pre_loaded_num_elements = state.range(0);
  auto indexes_count = state.range(1);
  LevelDbIndex index(indexes_count);

  // Fill in initial elements.
  for (std::int64_t i = 0; i < pre_loaded_num_elements; i++) {
    for (std::uint8_t j = 0; j < indexes_count; ++j) {
      auto& idx = index.GetIndex(j);
      *idx.GetOrAdd(ToKey(i));
    }
  }

  auto i = pre_loaded_num_elements;
  for (auto _ : state) {
    auto& idx = index.GetIndex(i % indexes_count);
    auto id = idx.GetOrAdd(ToKey(i));
    benchmark::DoNotOptimize(*id);
    ++i;
  }
}

BENCHMARK(BM_Insert<SingleIndex>)
    ->Args({1 << 10, 2})
    ->Args({1 << 20, 2})
    ->Args({1 << 10, 5})
    ->Args({1 << 20, 5})
    ->Args({1 << 10, 8})
    ->Args({1 << 20, 8});

BENCHMARK(BM_Insert<MultiIndex>)
    ->Args({1 << 10, 2})
    ->Args({1 << 20, 2})
    ->Args({1 << 10, 5})
    ->Args({1 << 20, 5})
    ->Args({1 << 10, 8})
    ->Args({1 << 20, 8});

template <typename LevelDbIndex>
void BM_SequentialRead(benchmark::State& state) {
  auto pre_loaded_num_elements = state.range(0);
  auto indexes_count = state.range(1);
  LevelDbIndex index(indexes_count);

  // Fill in initial elements.
  for (std::int64_t i = 0; i < pre_loaded_num_elements; i++) {
    for (std::uint8_t j = 0; j < indexes_count; ++j) {
      auto& idx = index.GetIndex(j);
      *idx.GetOrAdd(ToKey(i));
    }
  }

  auto i = 0;
  for (auto _ : state) {
    auto& idx = index.GetIndex(i % indexes_count);
    auto id = idx.GetOrAdd(ToKey(i % pre_loaded_num_elements));
    benchmark::DoNotOptimize(*id);
    ++i;
  }
}

BENCHMARK(BM_SequentialRead<SingleIndex>)
    ->Args({1 << 10, 2})
    ->Args({1 << 20, 2})
    ->Args({1 << 10, 5})
    ->Args({1 << 20, 5})
    ->Args({1 << 10, 8})
    ->Args({1 << 20, 8});

BENCHMARK(BM_SequentialRead<MultiIndex>)
    ->Args({1 << 10, 2})
    ->Args({1 << 20, 2})
    ->Args({1 << 10, 5})
    ->Args({1 << 20, 5})
    ->Args({1 << 10, 8})
    ->Args({1 << 20, 8});

template <typename LevelDbIndex>
void BM_UniformRandomRead(benchmark::State& state) {
  auto pre_loaded_num_elements = state.range(0);
  auto indexes_count = state.range(1);
  LevelDbIndex index(indexes_count);

  // Fill in initial elements.
  for (std::int64_t i = 0; i < pre_loaded_num_elements; i++) {
    for (std::uint8_t j = 0; j < indexes_count; ++j) {
      auto& idx = index.GetIndex(j);
      *idx.GetOrAdd(ToKey(i));
    }
  }

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dist(0, pre_loaded_num_elements - 1);
  for (auto _ : state) {
    auto i = dist(gen);
    auto& idx = index.GetIndex(i % indexes_count);
    auto id = idx.GetOrAdd(ToKey(i));
    benchmark::DoNotOptimize(*id);
  }
}

BENCHMARK(BM_UniformRandomRead<SingleIndex>)
    ->Args({1 << 10, 2})
    ->Args({1 << 20, 2})
    ->Args({1 << 10, 5})
    ->Args({1 << 20, 5})
    ->Args({1 << 10, 8})
    ->Args({1 << 20, 8});

BENCHMARK(BM_UniformRandomRead<MultiIndex>)
    ->Args({1 << 10, 2})
    ->Args({1 << 20, 2})
    ->Args({1 << 10, 5})
    ->Args({1 << 20, 5})
    ->Args({1 << 10, 8})
    ->Args({1 << 20, 8});

template <typename LevelDbIndex>
void BM_ExponentialRandomRead(benchmark::State& state) {
  auto pre_loaded_num_elements = state.range(0);
  auto indexes_count = state.range(1);
  LevelDbIndex index(indexes_count);

  // Fill in initial elements.
  for (std::int64_t i = 0; i < pre_loaded_num_elements; i++) {
    for (std::uint8_t j = 0; j < indexes_count; ++j) {
      auto& idx = index.GetIndex(j);
      *idx.GetOrAdd(ToKey(i));
    }
  }

  std::random_device rd;
  std::mt19937 gen(rd());
  std::exponential_distribution<> dist(double(10) / pre_loaded_num_elements);
  for (auto _ : state) {
    auto i = dist(gen);
    auto& idx = index.GetIndex(int(i) % indexes_count);
    auto id = idx.GetOrAdd(ToKey(std::int64_t(i) % pre_loaded_num_elements));
    benchmark::DoNotOptimize(*id);
  }
}

BENCHMARK(BM_ExponentialRandomRead<SingleIndex>)
    ->Args({1 << 10, 2})
    ->Args({1 << 20, 2})
    ->Args({1 << 10, 5})
    ->Args({1 << 20, 5})
    ->Args({1 << 10, 8})
    ->Args({1 << 20, 8});

BENCHMARK(BM_ExponentialRandomRead<MultiIndex>)
    ->Args({1 << 10, 2})
    ->Args({1 << 20, 2})
    ->Args({1 << 10, 5})
    ->Args({1 << 20, 5})
    ->Args({1 << 10, 8})
    ->Args({1 << 20, 8});

}  // namespace
}  // namespace carmen::backend::index
