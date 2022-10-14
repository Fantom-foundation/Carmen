#include <filesystem>
#include <random>
#include <sstream>

#include "backend/common/file.h"
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

// An utility wrapper to be specialized for various file implementations to
// handle them uniformely within benchmarks.
template <typename F>
class FileWrapper;

// A specialization of a FileWrapper for the InMemoryFile reference
// implementation.
template <std::size_t page_size>
class FileWrapper<InMemoryFile<page_size>> {
 public:
  InMemoryFile<page_size>& GetFile() { return file_; }

 private:
  InMemoryFile<page_size> file_;
};

// A specialization of a FileWrapper for the SingleFile implementation. In
// addition to maintaining a File instance, this wrapper also handles the
// ownership of a temporary file on disk backing owned file instance. In
// particular, it creates a temporary file when being instantiated and removes
// it upon destruction of the wrapper instance.
template <std::size_t page_size>
class FileWrapper<SingleFile<page_size>> {
 public:
  FileWrapper() : file_(std::make_unique<SingleFile<page_size>>(temp_file_)) {}
  ~FileWrapper() {
    file_->Flush();
    file_.reset();
    std::filesystem::remove(temp_file_);
  }
  SingleFile<page_size>& GetFile() { return *file_; }

 private:
  TempFile temp_file_;
  std::unique_ptr<SingleFile<page_size>> file_;
};

// Test the creation of files between 1 and 64 MiB.
constexpr long kMinSize = 1 << 20;
constexpr long kMaxSize = 1 << 26;

// A benchmark testing the initialization of an empty file with a given size.
template <typename F>
void BM_FileInit(benchmark::State& state) {
  constexpr static auto kPageSize = F::kPageSize;
  const auto target_size = state.range(0);
  using Page = std::array<std::byte, kPageSize>;

  for (auto _ : state) {
    // We create a file and only load the final page. This implicitly creates
    // the rest of the file.
    FileWrapper<F> wrapper;
    F& file = wrapper.GetFile();
    Page trg;
    file.LoadPage(target_size / kPageSize - 1, trg);
    benchmark::DoNotOptimize(trg[0]);
  }
}

BENCHMARK(BM_FileInit<InMemoryFile<256>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_FileInit<InMemoryFile<4096>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_FileInit<InMemoryFile<16384>>)->Range(kMinSize, kMaxSize);

BENCHMARK(BM_FileInit<SingleFile<256>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_FileInit<SingleFile<4096>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_FileInit<SingleFile<16384>>)->Range(kMinSize, kMaxSize);

// A benchmark testing the filling of a file with zeros by starting from an
// empty file and loading new pages in sequence.
template <typename F>
void BM_SequentialFileFilling(benchmark::State& state) {
  constexpr static auto kPageSize = F::kPageSize;
  const auto target_size = state.range(0);
  using Page = std::array<std::byte, kPageSize>;

  for (auto _ : state) {
    FileWrapper<F> wrapper;
    F& file = wrapper.GetFile();
    for (std::size_t i = 0; i < target_size / kPageSize; i++) {
      // Loading a page initializes the page to zero on disk.
      Page trg;
      file.LoadPage(i, trg);
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

// A benchmark testing the speed of reading pages sequentially.
template <typename F>
void BM_SequentialFileRead(benchmark::State& state) {
  constexpr static auto kPageSize = F::kPageSize;
  const auto target_size = state.range(0);
  using Page = std::array<std::byte, kPageSize>;

  // Create and initialize the test file.
  FileWrapper<F> wrapper;
  F& file = wrapper.GetFile();
  Page trg;
  const auto num_pages = target_size / kPageSize;
  file.LoadPage(num_pages - 1, trg);

  for (auto _ : state) {
    // Load all pages in order.
    for (std::size_t i = 0; i < num_pages; i++) {
      file.LoadPage(i, trg);
      benchmark::DoNotOptimize(trg[0]);
    }
  }
}

BENCHMARK(BM_SequentialFileRead<InMemoryFile<256>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_SequentialFileRead<InMemoryFile<4096>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_SequentialFileRead<InMemoryFile<16384>>)
    ->Range(kMinSize, kMaxSize);

BENCHMARK(BM_SequentialFileRead<SingleFile<256>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_SequentialFileRead<SingleFile<4096>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_SequentialFileRead<SingleFile<16384>>)->Range(kMinSize, kMaxSize);

// A benchmark testing the speed of reading pages randomly.
template <typename F>
void BM_RandomFileRead(benchmark::State& state) {
  constexpr static auto kPageSize = F::kPageSize;
  const auto target_size = state.range(0);
  using Page = std::array<std::byte, kPageSize>;

  // Create and initialize the test file.
  FileWrapper<F> wrapper;
  F& file = wrapper.GetFile();
  Page trg;
  const auto num_pages = target_size / kPageSize;
  file.LoadPage(num_pages - 1, trg);

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> distribution(0, num_pages - 1);

  for (auto _ : state) {
    // Load pages in random order.
    for (std::size_t i = 0; i < num_pages; i++) {
      file.LoadPage(distribution(gen), trg);
      benchmark::DoNotOptimize(trg[0]);
    }
  }
}

BENCHMARK(BM_RandomFileRead<InMemoryFile<256>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_RandomFileRead<InMemoryFile<4096>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_RandomFileRead<InMemoryFile<16384>>)->Range(kMinSize, kMaxSize);

BENCHMARK(BM_RandomFileRead<SingleFile<256>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_RandomFileRead<SingleFile<4096>>)->Range(kMinSize, kMaxSize);
BENCHMARK(BM_RandomFileRead<SingleFile<16384>>)->Range(kMinSize, kMaxSize);

}  // namespace
}  // namespace carmen::backend::store
