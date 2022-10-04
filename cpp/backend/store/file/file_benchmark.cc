

#include <filesystem>
#include <sstream>

#include "backend/store/file/file.h"
#include "benchmark/benchmark.h"
#include "common/file_util.h"

namespace carmen::backend::store {
namespace {

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

// Test the creation of files between 1 and 64 MiB.
constexpr int kMinSize = 1 << 20;
constexpr int kMaxSize = 1 << 26;

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

}  // namespace
}  // namespace carmen::backend::store
