#pragma once

#include <cstddef>
#include <filesystem>

#include "backend/common/file.h"
#include "backend/store/file/store.h"
#include "backend/store/memory/store.h"
#include "common/file_util.h"
#include "common/status_util.h"

namespace carmen::backend::store {
namespace {

// The reference store implementation type used to validate implementations.
template <std::size_t page_size>
using ReferenceStore = InMemoryStore<int, Value, page_size>;

// A generic store handler enclosing the setup and tear down of various store
// implementations for the generic unit tests in store_test.cc and benchmarks in
// store_benchmark.cc. A handler holds an instance of a store configured with a
// given page size and branching factor, as well as a reference store configured
// with the same parameters.
//
// This generic StoreHandler is a mere wrapper on a store reference, while
// specializations may add additional setup and tear-down operations.
template <typename Store, std::size_t branching_factor>
class StoreHandler {
 public:
  constexpr static std::size_t kPageSize = Store::kPageSize;
  constexpr static std::size_t kBranchingFactor = branching_factor;

  template <typename... Args>
  static absl::StatusOr<StoreHandler> Create(Args&&... args) {
    TempDir dir;
    Context ctx;
    ASSIGN_OR_RETURN(auto store,
                     Store::Open(ctx, dir.GetPath(), branching_factor,
                                 std::forward<Args>(args)...));
    return StoreHandler(std::move(store), std::move(ctx), std::move(dir));
  }

  Store& GetStore() { return store_; }

  auto& GetReferenceStore() { return reference_; }

 private:
  StoreHandler(Store store, Context context, TempDir dir)
      : dir_(std::move(dir)),
        context_(std::move(context)),
        store_(std::move(store)),
        reference_(branching_factor) {}

  TempDir dir_;
  Context context_;
  Store store_;
  ReferenceStore<Store::kPageSize> reference_;
};
}  // namespace
}  // namespace carmen::backend::store
