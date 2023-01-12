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
  static absl::StatusOr<IndexHandler> Create(Args&&... args) {
    TempDir dir;
    Context ctx;
    ASSIGN_OR_RETURN(auto index,
                     Index::Open(ctx, dir.GetPath(), std::forward<Args>(args)...));
    return IndexHandler(std::move(ctx), std::move(dir), std::move(index));
  }
  Index& GetIndex() { return index_; }

 private:
  IndexHandler(Context ctx, TempDir dir, Index idx) : ctx_(std::move(ctx)), temp_dir_(std::move(dir)), index_(std::move(idx)) {};

  Context ctx_;
  TempDir temp_dir_;
  Index index_;
};

// A specialization of the generic IndexHandler for leveldb implementation.
template <Trivial K, std::integral I>
class IndexHandler<LevelDbKeySpace<K, I>> : public IndexHandlerBase<K, I> {
 public:
  template <typename... Args>
  static absl::StatusOr<IndexHandler> Create(Args&&... args) {
    auto handler = IndexHandler();
    ASSIGN_OR_RETURN(auto index,
                     SingleLevelDbIndex::Open(handler.temp_dir_.GetPath(), std::forward<Args>(args)...));
    handler.index_ = index.template KeySpace<K, I>('t');
    return handler;
  }

  LevelDbKeySpace<K, I>& GetIndex() { return index_; }

 private:
  TempDir temp_dir_;
  LevelDbKeySpace<K, I> index_;
};
}  // namespace
}  // namespace carmen::backend::index
