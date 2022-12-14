#include <filesystem>
#include <random>
#include <sstream>

#include "backend/common/file.h"
#include "backend/common/page.h"
#include "benchmark/benchmark.h"
#include "common/file_util.h"
#include "common/status_test_util.h"

namespace carmen::backend::store {
namespace {

// Contains a range of benchmarks for the File concept implementations.
// To run benchmarks, use the following command:
//    bazel run -c opt //backend/common:file_benchmark
//
// To run subsets of benchmarks, add the filter flag --benchmark_filter=<regex>
// to the command line. Note that executable parameters must be separated from
// bazel parameters using a -- parameter. For instance, to only run Read
// benchmarks on the InMemoryFile implementation us
//    bazel run -c opt //backend/common:file_benchmark --
//    --benchmark_filter=Read.*InMemory
//
// The user Guide for the used benchmark infrastructure can be found here:
// https://github.com/google/benchmark/blob/main/docs/user_guide.md

// A page format used for the benchmarks.
template <std::size_t page_size>
using Page = ArrayPage<std::byte, page_size>;

// A utility wrapper to be specialized for various file implementations to
// handle them uniformly within benchmarks.
//
// The default implementation maintains a File instance and the ownership of a
// temporary file on disk backing the owned file instance. In particular, it
// creates a temporary file when being instantiated and removes it upon
// destruction of the wrapper instance.
template <typename F>
class FileWrapper {
 public:
  FileWrapper() : file_(std::make_unique<F>(temp_file_)) {}
  ~FileWrapper() {
    file_->Flush().IgnoreError();
    file_.reset();
    std::filesystem::remove(temp_file_);
  }
  F& GetFile() { return *file_; }

 private:
  TempFile temp_file_;
  std::unique_ptr<F> file_;
};

// A specialization of a FileWrapper for the InMemoryFile reference
// implementation.
template <std::size_t page_size>
class FileWrapper<InMemoryFile<Page<page_size>>> {
 public:
  InMemoryFile<Page<page_size>>& GetFile() { return file_; }

 private:
  InMemoryFile<Page<page_size>> file_;
};

template <::carmen::backend::Page Page>
using StreamFile = SingleFileBase<Page, internal::FStreamFile>;

template <::carmen::backend::Page Page>
using CFile = SingleFileBase<Page, internal::CFile>;

template <::carmen::backend::Page Page>
using PosixFile = SingleFileBase<Page, internal::PosixFile>;

// Test the creation of files between 1 and 64 MiB.
constexpr long kMinSize = 1 << 20;
constexpr long kMaxSize = 1 << 26;

// A benchmark testing the initialization of an empty file with a given size.
template <typename F>
void BM_FileInit(benchmark::State& state) {
  using Page = typename F::page_type;
  const auto target_size = state.range(0);

  for (auto _ : state) {
    // We create a file and only write the final page. This implicitly creates
    // the rest of the file.
    FileWrapper<F> wrapper;
    F& file = wrapper.GetFile();
    Page trg;
    ASSERT_OK(file.StorePage(target_size / sizeof(Page) - 1, trg));
    benchmark::DoNotOptimize(trg[0]);
  }
}

BENCHMARK(BM_FileInit<InMemoryFile<Page<256>>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_FileInit<InMemoryFile<Page<4096>>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_FileInit<InMemoryFile<Page<16384>>>)->Range(kMinSize, kMaxSize);

BENCHMARK(BM_FileInit<SingleFile<Page<256>>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_FileInit<SingleFile<Page<4096>>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_FileInit<SingleFile<Page<16384>>>)->Range(kMinSize, kMaxSize);

BENCHMARK(BM_FileInit<StreamFile<Page<256>>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_FileInit<StreamFile<Page<4096>>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_FileInit<StreamFile<Page<16384>>>)->Range(kMinSize, kMaxSize);

BENCHMARK(BM_FileInit<CFile<Page<256>>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_FileInit<CFile<Page<4096>>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_FileInit<CFile<Page<16384>>>)->Range(kMinSize, kMaxSize);

BENCHMARK(BM_FileInit<PosixFile<Page<256>>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_FileInit<PosixFile<Page<4096>>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_FileInit<PosixFile<Page<16384>>>)->Range(kMinSize, kMaxSize);

// A benchmark testing the filling of a file with zeros by starting from an
// empty file and loading new pages in sequence.
template <typename F>
void BM_SequentialFileFilling(benchmark::State& state) {
  using Page = typename F::page_type;
  const auto target_size = state.range(0);

  for (auto _ : state) {
    FileWrapper<F> wrapper;
    F& file = wrapper.GetFile();
    for (std::size_t i = 0; i < target_size / sizeof(Page); i++) {
      Page trg;
      ASSERT_OK(file.StorePage(i, trg));
      benchmark::DoNotOptimize(trg[0]);
    }
  }
}

BENCHMARK(BM_SequentialFileFilling<InMemoryFile<Page<256>>>)
    ->Range(kMinSize, kMaxSize);
BENCHMARK(BM_SequentialFileFilling<InMemoryFile<Page<4096>>>)
    ->Range(kMinSize, kMaxSize);
BENCHMARK(BM_SequentialFileFilling<InMemoryFile<Page<16384>>>)
    ->Range(kMinSize, kMaxSize);

BENCHMARK(BM_SequentialFileFilling<SingleFile<Page<256>>>)
    ->Range(kMinSize, kMaxSize);
BENCHMARK(BM_SequentialFileFilling<SingleFile<Page<4096>>>)
    ->Range(kMinSize, kMaxSize);
BENCHMARK(BM_SequentialFileFilling<SingleFile<Page<16384>>>)
    ->Range(kMinSize, kMaxSize);

BENCHMARK(BM_SequentialFileFilling<StreamFile<Page<256>>>)
    ->Range(kMinSize, kMaxSize);
BENCHMARK(BM_SequentialFileFilling<StreamFile<Page<4096>>>)
    ->Range(kMinSize, kMaxSize);
BENCHMARK(BM_SequentialFileFilling<StreamFile<Page<16384>>>)
    ->Range(kMinSize, kMaxSize);

BENCHMARK(BM_SequentialFileFilling<CFile<Page<256>>>)
    ->Range(kMinSize, kMaxSize);
BENCHMARK(BM_SequentialFileFilling<CFile<Page<4096>>>)
    ->Range(kMinSize, kMaxSize);
BENCHMARK(BM_SequentialFileFilling<CFile<Page<16384>>>)
    ->Range(kMinSize, kMaxSize);

BENCHMARK(BM_SequentialFileFilling<PosixFile<Page<256>>>)
    ->Range(kMinSize, kMaxSize);
BENCHMARK(BM_SequentialFileFilling<PosixFile<Page<4096>>>)
    ->Range(kMinSize, kMaxSize);
BENCHMARK(BM_SequentialFileFilling<PosixFile<Page<16384>>>)
    ->Range(kMinSize, kMaxSize);

// A benchmark testing the speed of reading pages sequentially.
template <typename F>
void BM_SequentialFileRead(benchmark::State& state) {
  using Page = typename F::page_type;
  const auto target_size = state.range(0);

  // Create and initialize the test file.
  FileWrapper<F> wrapper;
  F& file = wrapper.GetFile();
  Page trg;
  const auto num_pages = target_size / sizeof(Page);
  ASSERT_OK(file.StorePage(num_pages - 1, trg));

  int i = 0;
  for (auto _ : state) {
    // Load all pages in order.
    ASSERT_OK(file.LoadPage(i++ % num_pages, trg));
    benchmark::DoNotOptimize(trg[0]);
  }
}

BENCHMARK(BM_SequentialFileRead<InMemoryFile<Page<256>>>)
    ->Range(kMinSize, kMaxSize);
BENCHMARK(BM_SequentialFileRead<InMemoryFile<Page<4096>>>)
    ->Range(kMinSize, kMaxSize);
BENCHMARK(BM_SequentialFileRead<InMemoryFile<Page<16384>>>)
    ->Range(kMinSize, kMaxSize);

BENCHMARK(BM_SequentialFileRead<SingleFile<Page<256>>>)
    ->Range(kMinSize, kMaxSize);
BENCHMARK(BM_SequentialFileRead<SingleFile<Page<4096>>>)
    ->Range(kMinSize, kMaxSize);
BENCHMARK(BM_SequentialFileRead<SingleFile<Page<16384>>>)
    ->Range(kMinSize, kMaxSize);

BENCHMARK(BM_SequentialFileRead<StreamFile<Page<256>>>)
    ->Range(kMinSize, kMaxSize);
BENCHMARK(BM_SequentialFileRead<StreamFile<Page<4096>>>)
    ->Range(kMinSize, kMaxSize);
BENCHMARK(BM_SequentialFileRead<StreamFile<Page<16384>>>)
    ->Range(kMinSize, kMaxSize);

BENCHMARK(BM_SequentialFileRead<CFile<Page<256>>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_SequentialFileRead<CFile<Page<4096>>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_SequentialFileRead<CFile<Page<16384>>>)->Range(kMinSize, kMaxSize);

BENCHMARK(BM_SequentialFileRead<PosixFile<Page<256>>>)
    ->Range(kMinSize, kMaxSize);
BENCHMARK(BM_SequentialFileRead<PosixFile<Page<4096>>>)
    ->Range(kMinSize, kMaxSize);
BENCHMARK(BM_SequentialFileRead<PosixFile<Page<16384>>>)
    ->Range(kMinSize, kMaxSize);

// A benchmark testing the speed of reading pages randomly.
template <typename F>
void BM_RandomFileRead(benchmark::State& state) {
  using Page = typename F::page_type;
  const auto target_size = state.range(0);

  // Create and initialize the test file.
  FileWrapper<F> wrapper;
  F& file = wrapper.GetFile();
  Page trg;
  const auto num_pages = target_size / sizeof(Page);
  ASSERT_OK(file.StorePage(num_pages - 1, trg));

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> distribution(0, num_pages - 1);

  for (auto _ : state) {
    // Load pages in random order.
    ASSERT_OK(file.LoadPage(distribution(gen), trg));
    benchmark::DoNotOptimize(trg[0]);
  }
}

BENCHMARK(BM_RandomFileRead<InMemoryFile<Page<256>>>)
    ->Range(kMinSize, kMaxSize);
BENCHMARK(BM_RandomFileRead<InMemoryFile<Page<4096>>>)
    ->Range(kMinSize, kMaxSize);
BENCHMARK(BM_RandomFileRead<InMemoryFile<Page<16384>>>)
    ->Range(kMinSize, kMaxSize);

BENCHMARK(BM_RandomFileRead<SingleFile<Page<256>>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_RandomFileRead<SingleFile<Page<4096>>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_RandomFileRead<SingleFile<Page<16384>>>)
    ->Range(kMinSize, kMaxSize);

BENCHMARK(BM_RandomFileRead<StreamFile<Page<256>>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_RandomFileRead<StreamFile<Page<4096>>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_RandomFileRead<StreamFile<Page<16384>>>)
    ->Range(kMinSize, kMaxSize);

BENCHMARK(BM_RandomFileRead<CFile<Page<256>>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_RandomFileRead<CFile<Page<4096>>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_RandomFileRead<CFile<Page<16384>>>)->Range(kMinSize, kMaxSize);

BENCHMARK(BM_RandomFileRead<PosixFile<Page<256>>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_RandomFileRead<PosixFile<Page<4096>>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_RandomFileRead<PosixFile<Page<16384>>>)->Range(kMinSize, kMaxSize);

}  // namespace
}  // namespace carmen::backend::store
