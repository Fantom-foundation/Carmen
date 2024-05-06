// Copyright (c) 2024 Fantom Foundation
// 
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
// 
// Change Date: 2028-4-16
// 
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

#pragma once

#include <array>
#include <filesystem>
#include <string_view>
#include <utility>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "backend/common/leveldb/leveldb.h"
#include "backend/index/leveldb/index.h"
#include "backend/structure.h"

namespace carmen::backend::index {

// MultiLevelDbIndex is an index implementation over leveldb. Each index
// is supposed to be stored in a separate leveldb instance. Data is stored in
// the following format: key -> value.
template <Trivial K, std::integral I>
class MultiLevelDbIndex : public internal::LevelDbIndexBase<K, I, 0> {
 public:
  static absl::StatusOr<MultiLevelDbIndex> Open(
      const std::filesystem::path& path) {
    ASSIGN_OR_RETURN(auto db, LevelDb::Open(path));
    return MultiLevelDbIndex(std::move(db));
  }

  static absl::StatusOr<MultiLevelDbIndex> Open(
      Context&, const std::filesystem::path& path) {
    return Open(path);
  }

 private:
  explicit MultiLevelDbIndex(LevelDb ldb)
      : internal::LevelDbIndexBase<K, I, 0>(), ldb_(std::move(ldb)) {}

  std::string GetHashKey() const override { return "hash"; };

  std::string GetLastIndexKey() const override { return "last_index"; }

  std::array<char, sizeof(K)> ToDBKey(const K& key) const override {
    std::array<char, sizeof(K)> buffer;
    std::memcpy(buffer.data(), &key, sizeof(K));
    return buffer;
  };

  LevelDb& GetDb() override { return ldb_; }
  const LevelDb& GetDb() const override { return ldb_; }

  LevelDb ldb_;
};

}  // namespace carmen::backend::index
