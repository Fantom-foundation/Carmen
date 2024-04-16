/*
 * Copyright (c) 2024 Fantom Foundation
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at fantom.foundation/bsl11.
 *
 * Change Date: 2028-4-16
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by the GNU Lesser General Public Licence v3 
 */

#include <concepts>
#include <random>

#include "backend/index/leveldb/multi_db/index.h"
#include "backend/index/leveldb/single_db/index.h"
#include "benchmark/benchmark.h"
#include "common/file_util.h"
#include "common/status_test_util.h"
#include "common/type.h"

namespace carmen::backend::index {
namespace {

// To run benchmarks, use the following command:
//    bazel run -c opt //backend/index/leveldb:index_benchmark

template <Trivial K, std::integral I>
class SingleIndexBM {
 public:
  static absl::StatusOr<SingleIndexBM> Create(std::uint8_t num_indexes) {
    assert(num_indexes > 0 && "num_indexes must be greater than 0");
    TempDir dir;
    ASSIGN_OR_RETURN(auto index, SingleLevelDbIndex::Open(dir.GetPath()));
    return SingleIndexBM(num_indexes, index, std::move(dir));
  }

  LevelDbKeySpace<K, I>& GetIndex(std::uint8_t index) {
    return indexes_[index];
  }

 private:
  SingleIndexBM(std::uint8_t num_indexes, SingleLevelDbIndex& index,
                TempDir dir)
      : dir_(std::move(dir)) {
    // initialize index leveldb index
    for (std::uint8_t i = 0; i < num_indexes; ++i) {
      // create key space
      indexes_.push_back(index.template KeySpace<K, I>(i));
    }
  }

  TempDir dir_;
  std::vector<LevelDbKeySpace<K, I>> indexes_;
};

template <Trivial K, std::integral I>
class MultiIndexBM {
 public:
  static absl::StatusOr<MultiIndexBM> Create(std::uint8_t num_indexes) {
    using Index = MultiLevelDbIndex<K, I>;
    assert(num_indexes > 0 && "num_indexes must be greater than 0");
    std::vector<TempDir> dirs;
    std::vector<MultiLevelDbIndex<K, I>> indexes;
    for (std::uint8_t i = 0; i < num_indexes; ++i) {
      auto dir = TempDir();
      ASSIGN_OR_RETURN(auto index, Index::Open(dir.GetPath()));
      indexes.push_back(std::move(index));
      dirs.push_back(std::move(dir));
    }
    return MultiIndexBM(std::move(dirs), std::move(indexes));
  }

  MultiLevelDbIndex<K, I>& GetIndex(std::uint8_t index) {
    return indexes_[index];
  }

 private:
  MultiIndexBM(std::vector<TempDir> dirs,
               std::vector<MultiLevelDbIndex<K, I>> indexes)
      : dirs_(std::move(dirs)), indexes_(std::move(indexes)) {}

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
  ASSERT_OK_AND_ASSIGN(auto index, LevelDbIndex::Create(indexes_count));

  // Fill in initial elements.
  for (std::int64_t i = 0; i < pre_loaded_num_elements; i++) {
    for (std::uint8_t j = 0; j < indexes_count; ++j) {
      auto& idx = index.GetIndex(j);
      ASSERT_OK(idx.GetOrAdd(ToKey(i)));
    }
  }

  auto i = pre_loaded_num_elements;
  for (auto _ : state) {
    auto& idx = index.GetIndex(i % indexes_count);
    ASSERT_OK_AND_ASSIGN(auto id, idx.GetOrAdd(ToKey(i)));
    benchmark::DoNotOptimize(id);
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
  ASSERT_OK_AND_ASSIGN(auto index, LevelDbIndex::Create(indexes_count));

  // Fill in initial elements.
  for (std::int64_t i = 0; i < pre_loaded_num_elements; i++) {
    for (std::uint8_t j = 0; j < indexes_count; ++j) {
      auto& idx = index.GetIndex(j);
      ASSERT_OK(idx.GetOrAdd(ToKey(i)));
    }
  }

  auto i = 0;
  for (auto _ : state) {
    auto& idx = index.GetIndex(i % indexes_count);
    ASSERT_OK_AND_ASSIGN(auto id,
                         idx.GetOrAdd(ToKey(i % pre_loaded_num_elements)));
    benchmark::DoNotOptimize(id);
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
  ASSERT_OK_AND_ASSIGN(auto index, LevelDbIndex::Create(indexes_count));

  // Fill in initial elements.
  for (std::int64_t i = 0; i < pre_loaded_num_elements; i++) {
    for (std::uint8_t j = 0; j < indexes_count; ++j) {
      auto& idx = index.GetIndex(j);
      ASSERT_OK(idx.GetOrAdd(ToKey(i)));
    }
  }

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dist(0, pre_loaded_num_elements - 1);
  for (auto _ : state) {
    auto i = dist(gen);
    auto& idx = index.GetIndex(i % indexes_count);
    ASSERT_OK_AND_ASSIGN(auto id, idx.GetOrAdd(ToKey(i)));
    benchmark::DoNotOptimize(id);
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
  ASSERT_OK_AND_ASSIGN(auto index, LevelDbIndex::Create(indexes_count));

  // Fill in initial elements.
  for (std::int64_t i = 0; i < pre_loaded_num_elements; i++) {
    for (std::uint8_t j = 0; j < indexes_count; ++j) {
      auto& idx = index.GetIndex(j);
      ASSERT_OK(idx.GetOrAdd(ToKey(i)));
    }
  }

  std::random_device rd;
  std::mt19937 gen(rd());
  std::exponential_distribution<> dist(double(10) / pre_loaded_num_elements);
  for (auto _ : state) {
    auto i = dist(gen);
    auto& idx = index.GetIndex(int(i) % indexes_count);
    ASSERT_OK_AND_ASSIGN(auto id, idx.GetOrAdd(ToKey(std::int64_t(i) %
                                                     pre_loaded_num_elements)));
    benchmark::DoNotOptimize(id);
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
