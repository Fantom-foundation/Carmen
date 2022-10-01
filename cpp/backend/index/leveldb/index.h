#pragma once

#include <string>

#include "leveldb/db.h"
#include "leveldb/slice.h"

#include "common/hash.h"
#include "common/type.h"

namespace carmen::backend::index {

namespace {
constexpr std::string_view kHashKey = "hash";
constexpr std::string_view kLastIndexKey = "last";
constexpr leveldb::WriteOptions kWriteOptions = leveldb::WriteOptions();
constexpr leveldb::ReadOptions kReadOptions = leveldb::ReadOptions();

template <Trivial K>
leveldb::Slice ConvertToSlice(const K& key) {
  return {reinterpret_cast<const char*>(&key), sizeof(key)};
}

template <typename I>
I ParseResult(std::string* value) {
  return *reinterpret_cast<I*>(value->data());
}

template <typename I>
leveldb::Slice IndexToSlice(const I& index) {
  return {reinterpret_cast<const char*>(&index), sizeof(index)};
}

leveldb::Slice HashKeySlice() {
  return {kHashKey.begin(), kHashKey.size()};
}

leveldb::Slice LastIndexKeySlice() {
  return {kLastIndexKey.begin(), kLastIndexKey.size()};
}

}



template <Trivial K, typename I>
class LevelDBIndexImpl {
 public:
  LevelDBIndexImpl() = default;

  I GetOrAdd(const K& key) {
    auto sliceKey = ConvertToSlice(key);
    std::string value;

    leveldb::Status s = GetConnection()->Get(kReadOptions, sliceKey, &value);

    if (s.ok()) {
      return ParseResult<I>(value);
    }

    if (s.IsNotFound()) {
      leveldb::Status a = GetConnection()->Get(kReadOptions, LastIndexKeySlice(), &value);
      if (a.IsNotFound()) {
        SetLastIndexValue(0);
        GetConnection()->Put(kWriteOptions,
                             sliceKey,
                             ConvertToSlice(0));
        return 0;
      }
      if (a.ok()) {
        I last = ParseResult<I>(value) + 1;
        SetLastIndexValue(last);
        GetConnection()->Put(kWriteOptions,
                             sliceKey,
                             ConvertToSlice(last));
        return last;
      }
      // handle exception
    }

    // handle exception
  }

 private:
  std::unique_ptr<leveldb::DB> db_;


  leveldb::DB* GetConnection() {
    if (db_ != nullptr) return db_.get();
    InitializeConnection();
    return db_.get();
  }

  void SetLastIndexValue(const I& value) {
    auto status = GetConnection()->Put(kWriteOptions, LastIndexKeySlice(), ConvertToSlice(value));
    if (!status.ok()) {
      // handle exception
    }
  }

  void InitializeConnection() {
    // open database connection and store pointer into unique_ptr.
    leveldb::DB* db;
    leveldb::Options options;
    options.create_if_missing = true;
    leveldb::Status status = leveldb::DB::Open(options, "/tmp/testdb", &db);
    db_.reset(db);
  }
};

}  // namespace carmen::backend::index
