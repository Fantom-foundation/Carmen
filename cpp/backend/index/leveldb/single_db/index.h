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

#include <filesystem>
#include <iostream>
#include <queue>
#include <sstream>
#include <string>
#include <utility>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "backend/common/leveldb/leveldb.h"
#include "backend/index/leveldb/index.h"
#include "backend/structure.h"
#include "common/hash.h"
#include "common/status_util.h"
#include "common/type.h"

namespace carmen::backend::index {
namespace internal {

// Converts given key space and key into leveldb key.
template <Trivial K>
std::array<char, sizeof(K) + 1> ToDBKey(char key_space, const K& key) {
  std::array<char, sizeof(key) + 1> buffer{key_space};
  memcpy(buffer.data() + 1, &key, sizeof(key));
  return buffer;
}

// Converts given key space and key into leveldb key.
std::string StrToDBKey(char key_space, std::span<const char> key);
}  // namespace internal

template <Trivial K, std::integral I>
class LevelDbKeySpace : public internal::LevelDbIndexBase<K, I, 1> {
 public:
  // A factory function creating an instance of this index type.
  static absl::StatusOr<LevelDbKeySpace> Open(
      Context& context, const std::filesystem::path& path) {
    // Obtain shared LevelDB instance from context or create and register one if
    // there is none so far.
    using SharedLevelDb = std::shared_ptr<LevelDb>;
    if (!context.HasComponent<SharedLevelDb>()) {
      ASSIGN_OR_RETURN(auto db, LevelDb::Open(path / "common_level_db",
                                              /*create_if_missing=*/true));
      context.RegisterComponent(std::make_shared<LevelDb>(std::move(db)));
    }
    auto ldb = context.GetComponent<SharedLevelDb>();

    // Next, we need to find a proper key space for this instance.
    // TODO: this is not pretty, should be improved.
    char key_space;
    if constexpr (std::is_same_v<K, Address>) {
      key_space = 'A';
    } else if constexpr (std::is_same_v<K, Key>) {
      key_space = 'K';
    } else {
      assert(false && "Unable to map value type to keyspace");
    }
    return LevelDbKeySpace(std::move(ldb), key_space);
  }

  LevelDbKeySpace(std::shared_ptr<LevelDb> ldb, char key_space)
      : internal::LevelDbIndexBase<K, I, 1>(),
        ldb_(std::move(ldb)),
        key_space_(key_space) {}

 private:
  std::string GetHashKey() const override {
    return internal::StrToDBKey(key_space_, "_hash");
  };

  std::string GetLastIndexKey() const override {
    return internal::StrToDBKey(key_space_, "_last_index");
  }

  std::array<char, sizeof(K) + 1> ToDBKey(const K& key) const override {
    return internal::ToDBKey(key_space_, key);
  };

  LevelDb& GetDb() override { return *ldb_; }
  const LevelDb& GetDb() const override { return *ldb_; }

  std::shared_ptr<LevelDb> ldb_;
  char key_space_;
};

// SingleLevelDbIndex is an index implementation over leveldb. It uses a single
// file to store all the data. Data is stored in the following format:
// key_space + key -> value.
class SingleLevelDbIndex {
 public:
  static absl::StatusOr<SingleLevelDbIndex> Open(
      const std::filesystem::path& path);

  // Returns index for given key space.
  template <Trivial K, std::integral I>
  LevelDbKeySpace<K, I> KeySpace(char key_space) {
    return {ldb_, key_space};
  }

 private:
  explicit SingleLevelDbIndex(std::shared_ptr<LevelDb> ldb);
  std::shared_ptr<LevelDb> ldb_;
};

}  // namespace carmen::backend::index
