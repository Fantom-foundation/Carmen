#include <cstddef>

#include "backend/index/index.h"
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

}  // namespace
}  // namespace carmen::backend::index
