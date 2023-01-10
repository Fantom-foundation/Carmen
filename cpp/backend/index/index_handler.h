#pragma once

#include <cstddef>

#include "backend/index/cache/cache.h"
#include "backend/index/file/index.h"
#include "backend/index/index.h"
#include "backend/index/leveldb/multi_db/index.h"
#include "backend/index/leveldb/single_db/index.h"
#include "backend/index/memory/index.h"
#include "common/file_util.h"
#include "common/type.h"
#include "absl/status/statusor.h"
#include "backend/structure.h"

namespace carmen::backend::index {
namespace {

// A base type for IndexHandlerBase types (see below) exposing common
// definitions.
template <Trivial K, std::integral I>
class IndexHandlerBase {
 public:
  auto& GetReferenceIndex() { return reference_; }

 private:
  InMemoryIndex<K, I> reference_;
};

// A generic index handler enclosing the setup and tear down of various index
// implementations in benchmarks handled by index_benchmark.cc. A handler holds
// an instance of an index.
//
// This generic IndexHandler is a mere wrapper on a store reference, while
// specializations may add additional setup and tear-down operations.
template <Index Index>
class IndexHandler : public IndexHandlerBase<typename Index::key_type,
                                             typename Index::value_type> {
 public:
  template <typename... Args>
  absl::StatusOr<IndexHandler> Create(Args&&... args) {
    auto handler = IndexHandler();
    ASSIGN_OR_RETURN(handler.index_,
                     Index::Open(handler.ctx, handler.temp_dir_.GetPath(), std::forward<Args>(args)...));
    return handler;
  }

  Index& GetIndex() { return index_; }

 private:
  Context ctx_;
  TempDir temp_dir_;
  Index index_;
};
/*
// A specialization of the generic IndexHandler for cached index
// implementations.
template <Index Index>
class IndexHandler<Cached<Index>>
    : public IndexHandlerBase<typename Index::key_type,
                              typename Index::value_type> {
 public:
  IndexHandler() : index_(std::move(nested_.GetIndex())) {}
  Cached<Index>& GetIndex() { return index_; }

 private:
  IndexHandler<Index> nested_;
  Cached<Index> index_;
};

// A specialization of the generic IndexHandler for file-based implementations.
template <Trivial K, std::integral I, std::size_t page_size>
class IndexHandler<FileIndex<K, I, SingleFile, page_size>>
    : public IndexHandlerBase<K, I> {
 public:
  using File = typename FileIndex<K, I, SingleFile, page_size>::File;
  IndexHandler() : index_(dir_.GetPath()) {}

  FileIndex<K, I, SingleFile, page_size>& GetIndex() { return index_; }

 private:
  TempDir dir_;
  FileIndex<K, I, SingleFile, page_size> index_;
};
*/
// A specialization of the generic IndexHandler for leveldb implementation.
template <Trivial K, std::integral I>
class IndexHandler<LevelDbKeySpace<K, I>> : public IndexHandlerBase<K, I> {
 public:
  IndexHandler()
      : index_((*SingleLevelDbIndex::Open(dir_.GetPath()))
                   .template KeySpace<K, I>('t')) {}
  LevelDbKeySpace<K, I>& GetIndex() { return index_; }

 private:
  TempDir dir_;
  LevelDbKeySpace<K, I> index_;
};
/*
// A specialization of the generic IndexHandler for leveldb implementation.
template <Trivial K, std::integral I>
class IndexHandler<MultiLevelDbIndex<K, I>> : public IndexHandlerBase<K, I> {
 public:
  IndexHandler() : index_(*MultiLevelDbIndex<K, I>::Open(dir_.GetPath())) {}
  MultiLevelDbIndex<K, I>& GetIndex() { return index_; }

 private:
  TempDir dir_;
  MultiLevelDbIndex<K, I> index_;
};
*/
}  // namespace
}  // namespace carmen::backend::index
