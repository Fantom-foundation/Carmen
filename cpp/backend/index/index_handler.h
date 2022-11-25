#pragma once

#include <cstddef>

#include "backend/index/cache/cache.h"
#include "backend/index/file/index.h"
#include "backend/index/index.h"
#include "backend/index/leveldb/multi_db/index.h"
#include "backend/index/leveldb/multi_db/test_util.h"
#include "backend/index/leveldb/single_db/index.h"
#include "backend/index/leveldb/single_db/test_util.h"
#include "common/file_util.h"
#include "common/type.h"

namespace carmen::backend::index {
namespace {

// A generic index handler enclosing the setup and tear down of various index
// implementations in benchmarks handled by index_benchmark.cc. A handler holds
// an instance of an index.
//
// This generic IndexHandler is a mere wrapper on a store reference, while
// specializations may add additional setup and tear-down operations.
template <Index Index>
class IndexHandler {
 public:
  IndexHandler() : index_() {}
  Index& GetIndex() { return index_; }

 private:
  Index index_;
};

// A specialization of the generic IndexHandler for cached index
// implementations.
template <Index Index>
class IndexHandler<Cached<Index>> {
 public:
  IndexHandler() : index_(std::move(nested_.GetIndex())) {}
  Cached<Index>& GetIndex() { return index_; }

 private:
  IndexHandler<Index> nested_;
  Cached<Index> index_;
};

// A specialization of the generic IndexHandler for file-based implementations.
template <Trivial K, std::integral I, std::size_t page_size>
class IndexHandler<FileIndex<K, I, SingleFile, page_size>> {
 public:
  using File = typename FileIndex<K, I, SingleFile, page_size>::File;
  IndexHandler() : index_(dir_.GetPath()) {}

  FileIndex<K, I, SingleFile, page_size>& GetIndex() { return index_; }

 private:
  TempDir dir_;
  FileIndex<K, I, SingleFile, page_size> index_;
};

// A specialization of the generic IndexHandler for leveldb implementation.
template <Trivial K, std::integral I>
class IndexHandler<SingleLevelDbIndexTestAdapter<K, I>> {
 public:
  IndexHandler()
      : index_((*SingleLevelDbIndex::Open(dir_.GetPath()))
                   .template KeySpace<K, I>('t')) {}
  SingleLevelDbIndexTestAdapter<K, I>& GetIndex() { return index_; }

 private:
  TempDir dir_;
  SingleLevelDbIndexTestAdapter<K, I> index_;
};

// A specialization of the generic IndexHandler for leveldb implementation.
template <Trivial K, std::integral I>
class IndexHandler<MultiLevelDbIndexTestAdapter<K, I>> {
 public:
  IndexHandler() : index_(*MultiLevelDbIndex<K, I>::Open(dir_.GetPath())) {}
  MultiLevelDbIndexTestAdapter<K, I>& GetIndex() { return index_; }

 private:
  TempDir dir_;
  MultiLevelDbIndexTestAdapter<K, I> index_;
};

}  // namespace
}  // namespace carmen::backend::index
