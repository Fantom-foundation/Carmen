#include <cstddef>

#include "backend/index/index.h"
#include "backend/index/leveldb/test_util.h"
#include "backend/index/leveldb/index.h"
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

// A specialization of the generic IndexHandler for leveldb implementation.
template <Trivial K, std::integral I>
class IndexHandler<LevelDBKeySpaceTestAdapter<K, I>> {
 public:
  IndexHandler() : dir_{}, index_(LevelDBIndex(dir_.GetPath().string()).KeySpace<int, int>('t')) {}
  LevelDBKeySpaceTestAdapter<K, I>& GetIndex() { return index_; }

 private:
  TempDir dir_;
  LevelDBKeySpaceTestAdapter<K, I> index_;
};

}  // namespace
}  // namespace carmen::backend::index
