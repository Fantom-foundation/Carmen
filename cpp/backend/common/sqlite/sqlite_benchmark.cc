// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

#include <array>

#include "backend/common/access_pattern.h"
#include "backend/common/sqlite/sqlite.h"
#include "benchmark/benchmark.h"
#include "common/file_util.h"
#include "common/status_test_util.h"

namespace carmen::backend {
namespace {

// To run benchmarks, use the following command:
//    bazel run -c opt //backend/common/sqlite:sqlite_benchmark

template <typename Distribution, bool use_transaction>
void IntInsertion(benchmark::State& state) {
  TempFile file;
  ASSERT_OK_AND_ASSIGN(auto db, Sqlite::Open(file));
  ASSERT_OK(db.Run("CREATE TABLE test (id INTEGER PRIMARY KEY) WITHOUT ROWID"));
  ASSERT_OK_AND_ASSIGN(auto insert,
                       db.Prepare("INSERT OR IGNORE INTO test(id) VALUES (?)"));

  Distribution distribution(1'000'000'000);
  if (use_transaction) {
    ASSERT_OK(db.Run("BEGIN TRANSACTION"));
  }
  for (auto _ : state) {
    int next = distribution.Next();
    ASSERT_OK(insert.Reset());
    ASSERT_OK(insert.Bind(0, next));
    ASSERT_OK(insert.Run());
  }
  if (use_transaction) {
    ASSERT_OK(db.Run("END TRANSACTION"));
  }
}

void BM_OrderedIntInsertionWithoutTransaction(benchmark::State& state) {
  IntInsertion<Sequential, false>(state);
}

BENCHMARK(BM_OrderedIntInsertionWithoutTransaction);

void BM_OrderedIntInsertionWithTransaction(benchmark::State& state) {
  IntInsertion<Sequential, true>(state);
}

BENCHMARK(BM_OrderedIntInsertionWithTransaction);

template <typename Distribution>
void BM_IntInsertion(benchmark::State& state) {
  IntInsertion<Distribution, true>(state);
}

BENCHMARK(BM_IntInsertion<Sequential>);
BENCHMARK(BM_IntInsertion<Uniform>);
BENCHMARK(BM_IntInsertion<Exponential>);

template <typename Distribution>
void BM_32ByteValueInsertion(benchmark::State& state) {
  using Value = std::array<std::byte, 32>;
  TempFile file;
  ASSERT_OK_AND_ASSIGN(auto db, Sqlite::Open(file));
  ASSERT_OK(db.Run("CREATE TABLE test (id BLOB PRIMARY KEY) WITHOUT ROWID"));
  ASSERT_OK_AND_ASSIGN(auto insert,
                       db.Prepare("INSERT OR IGNORE INTO test(id) VALUES (?)"));

  Value value;
  Distribution distribution(1'000'000'000);
  ASSERT_OK(db.Run("BEGIN TRANSACTION"));
  for (auto _ : state) {
    int next = distribution.Next();
    value[7] = static_cast<std::byte>(next);
    value[15] = static_cast<std::byte>(next >> 8);
    value[23] = static_cast<std::byte>(next >> 16);
    value[31] = static_cast<std::byte>(next >> 24);
    ASSERT_OK(insert.Reset());
    ASSERT_OK(insert.Bind(0, value));
    ASSERT_OK(insert.Run());
  }
  ASSERT_OK(db.Run("END TRANSACTION"));
}

BENCHMARK(BM_32ByteValueInsertion<Sequential>);
BENCHMARK(BM_32ByteValueInsertion<Uniform>);
BENCHMARK(BM_32ByteValueInsertion<Exponential>);

template <typename Distribution>
void BM_MultipleIntegerKeyInsertion(benchmark::State& state) {
  // This test setup inserts 32-byte values by deviding them into 4 8-byte
  // integer values.
  TempFile file;
  ASSERT_OK_AND_ASSIGN(auto db, Sqlite::Open(file));
  ASSERT_OK(
      db.Run("CREATE TABLE test (c0 INTEGER, c1 INTEGER, c2 INTEGER, c3 "
             "INTEGER, PRIMARY KEY (c0,c1,c2,c3)) WITHOUT ROWID"));
  ASSERT_OK_AND_ASSIGN(
      auto insert,
      db.Prepare("INSERT OR IGNORE INTO test(c0,c1,c2,c3) VALUES (?,?,?,?)"));

  Distribution distribution(1'000'000'000);
  ASSERT_OK(db.Run("BEGIN TRANSACTION"));
  for (auto _ : state) {
    std::int64_t next = distribution.Next();
    ASSERT_OK(insert.Reset());
    ASSERT_OK(insert.Bind(0, next));
    ASSERT_OK(insert.Bind(1, next));
    ASSERT_OK(insert.Bind(2, next));
    ASSERT_OK(insert.Bind(3, next));
    ASSERT_OK(insert.Run());
  }
  ASSERT_OK(db.Run("END TRANSACTION"));
}

BENCHMARK(BM_MultipleIntegerKeyInsertion<Sequential>);
BENCHMARK(BM_MultipleIntegerKeyInsertion<Uniform>);
BENCHMARK(BM_MultipleIntegerKeyInsertion<Exponential>);

}  // namespace
}  // namespace carmen::backend
