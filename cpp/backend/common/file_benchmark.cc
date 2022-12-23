#include <filesystem>
#include <random>
#include <sstream>

#include "backend/common/file.h"
#include "backend/common/page.h"
#include "benchmark/benchmark.h"
#include "common/file_util.h"

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
using Page = RawPage<page_size>;

// An utility wrapper to be specialized for various file implementations to
// handle them uniformely within benchmarks.
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
    file_->Flush();
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
class FileWrapper<InMemoryFile<page_size>> {
 public:
  InMemoryFile<page_size>& GetFile() { return file_; }

 private:
  InMemoryFile<page_size> file_;
};

template <std::size_t page_size>
using StreamFile = SingleFileBase<page_size, internal::FStreamFile>;

template <std::size_t page_size>
using CFile = SingleFileBase<page_size, internal::CFile>;

template <std::size_t page_size>
using PosixFile = SingleFileBase<page_size, internal::PosixFile>;

// Test the creation of files between 1 and 64 MiB.
constexpr long kMinSize = 1 << 20;
constexpr long kMaxSize = 1 << 26;

// A benchmark testing the initialization of an empty file with a given size.
template <typename F>
void BM_FileInit(benchmark::State& state) {
  const auto target_size = state.range(0);

  for (auto _ : state) {
    // We create a file and only write the final page. This implicitly creates
    // the rest of the file.
    FileWrapper<F> wrapper;
    F& file = wrapper.GetFile();
    Page<F::kPageSize> trg;
    file.StorePage(target_size / sizeof(trg) - 1, trg);
    benchmark::DoNotOptimize(trg[0]);
  }
}

BENCHMARK(BM_FileInit<InMemoryFile<256>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_FileInit<InMemoryFile<4096>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_FileInit<InMemoryFile<16384>>)->Range(kMinSize, kMaxSize);

BENCHMARK(BM_FileInit<SingleFile<256>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_FileInit<SingleFile<4096>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_FileInit<SingleFile<16384>>)->Range(kMinSize, kMaxSize);

BENCHMARK(BM_FileInit<StreamFile<256>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_FileInit<StreamFile<4096>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_FileInit<StreamFile<16384>>)->Range(kMinSize, kMaxSize);

BENCHMARK(BM_FileInit<CFile<256>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_FileInit<CFile<4096>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_FileInit<CFile<16384>>)->Range(kMinSize, kMaxSize);

BENCHMARK(BM_FileInit<PosixFile<256>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_FileInit<PosixFile<4096>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_FileInit<PosixFile<16384>>)->Range(kMinSize, kMaxSize);

// A benchmark testing the filling of a file with zeros by starting from an
// empty file and loading new pages in sequence.
template <typename F>
void BM_SequentialFileFilling(benchmark::State& state) {
  const auto target_size = state.range(0);

  for (auto _ : state) {
    FileWrapper<F> wrapper;
    F& file = wrapper.GetFile();
    for (std::size_t i = 0; i < target_size / F::kPageSize; i++) {
      Page<F::kPageSize> trg;
      file.StorePage(i, trg);
      benchmark::DoNotOptimize(trg[0]);
    }
  }
}

BENCHMARK(BM_SequentialFileFilling<InMemoryFile<256>>)
    ->Range(kMinSize, kMaxSize);
BENCHMARK(BM_SequentialFileFilling<InMemoryFile<4096>>)
    ->Range(kMinSize, kMaxSize);
BENCHMARK(BM_SequentialFileFilling<InMemoryFile<16384>>)
    ->Range(kMinSize, kMaxSize);

BENCHMARK(BM_SequentialFileFilling<SingleFile<256>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_SequentialFileFilling<SingleFile<4096>>)
    ->Range(kMinSize, kMaxSize);
BENCHMARK(BM_SequentialFileFilling<SingleFile<16384>>)
    ->Range(kMinSize, kMaxSize);

BENCHMARK(BM_SequentialFileFilling<StreamFile<256>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_SequentialFileFilling<StreamFile<4096>>)
    ->Range(kMinSize, kMaxSize);
BENCHMARK(BM_SequentialFileFilling<StreamFile<16384>>)
    ->Range(kMinSize, kMaxSize);

BENCHMARK(BM_SequentialFileFilling<CFile<256>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_SequentialFileFilling<CFile<4096>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_SequentialFileFilling<CFile<16384>>)->Range(kMinSize, kMaxSize);

BENCHMARK(BM_SequentialFileFilling<PosixFile<256>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_SequentialFileFilling<PosixFile<4096>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_SequentialFileFilling<PosixFile<16384>>)
    ->Range(kMinSize, kMaxSize);

// A benchmark testing the speed of reading pages sequentially.
template <typename F>
void BM_SequentialFileRead(benchmark::State& state) {
  const auto target_size = state.range(0);

  // Create and initialize the test file.
  FileWrapper<F> wrapper;
  F& file = wrapper.GetFile();
  Page<F::kPageSize> trg;
  const auto num_pages = target_size / F::kPageSize;
  file.StorePage(num_pages - 1, trg);

  int i = 0;
  for (auto _ : state) {
    // Load all pages in order.
    file.LoadPage(i++ % num_pages, trg);
    benchmark::DoNotOptimize(trg[0]);
  }
}

BENCHMARK(BM_SequentialFileRead<InMemoryFile<256>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_SequentialFileRead<InMemoryFile<4096>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_SequentialFileRead<InMemoryFile<16384>>)
    ->Range(kMinSize, kMaxSize);

BENCHMARK(BM_SequentialFileRead<SingleFile<256>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_SequentialFileRead<SingleFile<4096>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_SequentialFileRead<SingleFile<16384>>)->Range(kMinSize, kMaxSize);

BENCHMARK(BM_SequentialFileRead<StreamFile<256>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_SequentialFileRead<StreamFile<4096>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_SequentialFileRead<StreamFile<16384>>)->Range(kMinSize, kMaxSize);

BENCHMARK(BM_SequentialFileRead<CFile<256>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_SequentialFileRead<CFile<4096>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_SequentialFileRead<CFile<16384>>)->Range(kMinSize, kMaxSize);

BENCHMARK(BM_SequentialFileRead<PosixFile<256>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_SequentialFileRead<PosixFile<4096>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_SequentialFileRead<PosixFile<16384>>)->Range(kMinSize, kMaxSize);

// A benchmark testing the speed of reading pages randomly.
template <typename F>
void BM_RandomFileRead(benchmark::State& state) {
  const auto target_size = state.range(0);

  // Create and initialize the test file.
  FileWrapper<F> wrapper;
  F& file = wrapper.GetFile();
  Page<F::kPageSize> trg;
  const auto num_pages = target_size / F::kPageSize;
  file.StorePage(num_pages - 1, trg);

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> distribution(0, num_pages - 1);

  for (auto _ : state) {
    // Load pages in random order.
    file.LoadPage(distribution(gen), trg);
    benchmark::DoNotOptimize(trg[0]);
  }
}

BENCHMARK(BM_RandomFileRead<InMemoryFile<256>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_RandomFileRead<InMemoryFile<4096>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_RandomFileRead<InMemoryFile<16384>>)->Range(kMinSize, kMaxSize);

BENCHMARK(BM_RandomFileRead<SingleFile<256>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_RandomFileRead<SingleFile<4096>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_RandomFileRead<SingleFile<16384>>)->Range(kMinSize, kMaxSize);

BENCHMARK(BM_RandomFileRead<StreamFile<256>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_RandomFileRead<StreamFile<4096>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_RandomFileRead<StreamFile<16384>>)->Range(kMinSize, kMaxSize);

BENCHMARK(BM_RandomFileRead<CFile<256>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_RandomFileRead<CFile<4096>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_RandomFileRead<CFile<16384>>)->Range(kMinSize, kMaxSize);

BENCHMARK(BM_RandomFileRead<PosixFile<256>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_RandomFileRead<PosixFile<4096>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_RandomFileRead<PosixFile<16384>>)->Range(kMinSize, kMaxSize);

}  // namespace
}  // namespace carmen::backend::store
