#pragma once

#include <cstddef>
#include <filesystem>

#include "backend/common/file.h"
#include "backend/store/file/store.h"
#include "backend/store/memory/store.h"
#include "common/file_util.h"

namespace carmen::backend::store {
namespace {

// The reference store implementation type used to validate implementations.
template <std::size_t page_size>
using ReferenceStore = InMemoryStore<int, Value, page_size>;

// A base type for StoreHandler types (see below) exposing common definitions.
template <std::size_t page_size, std::size_t branching_factor>
class StoreHandlerBase {
 public:
  constexpr static std::size_t kPageSize = page_size;
  constexpr static std::size_t kBranchingFactor = branching_factor;

  StoreHandlerBase() : reference_(branching_factor) {}

  // Obtains access to a reference store implementation to be used to compare
  // the handled store with. The reference type is configured to use the same
  // page size and branching factor.
  auto& GetReferenceStore() { return reference_; }

  // A temporary directory files of the maintained store a placed in.
  std::filesystem::path GetStoreDirectory() const { return dir_; }

 private:
  ReferenceStore<page_size> reference_;
  TempDir dir_;
};

// A generic store handler enclosing the setup and tear down of various store
// implementations for the generic unit tests in store_test.cc and benchmarks in
// store_benchmark.cc. A handler holds an instance of a store configured with a
// given page size and branching factor, as well as a reference store configured
// with the same parameters.
//
// This generic StoreHandler is a mere wrapper on a store reference, while
// specializations may add additional setup and tear-down operations.
template <typename Store, std::size_t branching_factor>
class StoreHandler
    : public StoreHandlerBase<Store::kPageSize, branching_factor> {
 public:
  using StoreHandlerBase<Store::kPageSize, branching_factor>::GetStoreDirectory;
  StoreHandler() : store_(GetStoreDirectory(), branching_factor) {}
  Store& GetStore() { return store_; }

 private:
  Store store_;
};

// A specialization of a StoreHandler for InMemoryStores handling ingoring the
// creation/deletion of temporary files and directories.
template <typename Key, Trivial Value, std::size_t page_size,
          std::size_t branching_factor>
class StoreHandler<InMemoryStore<Key, Value, page_size>, branching_factor>
    : public StoreHandlerBase<page_size, branching_factor> {
 public:
  StoreHandler() : store_(branching_factor) {}

  InMemoryStore<Key, Value, page_size>& GetStore() { return store_; }

 private:
  InMemoryStore<Key, Value, page_size> store_;
};

}  // namespace
}  // namespace carmen::backend::store
